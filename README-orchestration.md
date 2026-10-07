# 本地测试执行编排（Test Orchestration）

本模块在 `com.github.guoyyit.testorchestrationjava.orchestration` 下实现了一套**纯 Java、本地多线程**的测试执行编排能力：把大量测试用例规划到多个执行单元（unit），按依赖、互斥与资源配额调度，处理不稳定用例重试/隔离，并对崩溃、取消、超时做失败治理，最终产出确定且可审计的汇总结果。

不依赖任何外部调度系统或消息队列；每个执行单元是一个工作线程，因此非常适合单机构建、CI 分片预览与教学/验证场景。

---

## 1. 快速上手

```java
OrchestrationConfig config = OrchestrationConfig.builder()
        .unitCount(4)                 // 4 个执行单元
        .maxConcurrency(4)            // 全局最多 4 个用例并发
        .maxRetries(2)                // 默认重试次数（首次执行 + 2 次重试）
        .flakyThreshold(2)            // 2 次“抖动运行”后认定为不稳定
        .flakyMaxRetries(4)           // 不稳定用例的重试预算
        .isolationStrategy(IsolationStrategy.QUARANTINE_AFTER)
        .defaultTimeoutMillis(30_000) // 单用例超时
        .runTimeoutMillis(600_000)    // 整次运行超时（0 = 不限）
        .seed(20261007L)              // 随机种子：决定分片的平局裁决
        .resourceQuotas(Map.of("db", 1, "gpu", 2))
        .build();

List<TestCase> cases = List.of(
    new TestCase("login", 120),
    new TestCase("checkout", 400, Set.of("db"), Set.of("serial"), List.of("login")));

TestExecutor executor = cases -> Map.of(
        "login",    (view, ctx) -> { /* ... */ },
        "checkout", (view, ctx) -> { /* ... */ });

RunResult result = new TestOrchestrator(config)
        .runWithAudit("build-42", cases, executor, Path.of("audit/build-42.json"));

Set<String> passed  = result.passedIds();
Set<String> failed  = result.failedIds();
Set<String> skipped = result.skippedIds();
```

`TestCase(id, historicalDurationMillis, resourceLabels, mutexGroups, dependsOn)` 声明一个用例：

- `resourceLabels`：占用的资源标签（如 `db`、`gpu`），运行时受配额约束；
- `mutexGroups`：互斥组 id，**同组用例绝不并发**（等价于该组配额为 1）；
- `dependsOn`：前置依赖 id 列表，依赖未 PASSED 时本用例被跳过。

测试体通过 `TestExecutable.execute(TestCaseView, ExecutionContext)` 表达：

- 抛 `AssertionError`：**确定性失败，绝不重试**；
- 抛 `TestFailureException(FailureType.RETRIABLE, ...)`：环境/抖动失败，可重试；
- 抛 `UnitCrashException`：模拟执行单元崩溃（见第 5 节）；
- 抛 `InterruptedException` / 线程被中断：超时或取消信号。

---

## 2. 分片（Sharding）语义

分片由 `ShardPlanner.plan(cases, unitCount, seed)` 完成，它是一个**纯函数**：

> 相同的（按 id 确定的）用例集合、相同的执行单元数量、相同的种子 ⇒ 永远得到相同的 `ShardPlan`。

### 2.1 算法

1. **校验与拓扑排序**：检查重复 id、未知依赖、重复依赖与依赖环（有环直接抛异常）；用 Kahn 算法做拓扑排序，就绪集合按 id 排序，保证顺序不依赖输入列表的偶然排列。
2. **贪心 LPT（Longest Processing Time）装箱**：按拓扑顺序逐个把用例放入“放入后负载最小”的执行单元。
3. **负载评分（weighted by time and resource）**：单元的得分是以下二者的最大值：
   - 分片上历史耗时总和；
   - 该分片上任意**单一资源标签**的历史耗时总和。

   这样一批都吃同一个稀缺资源（比如唯一的 `db`）的重用例会被摊开，而不是堆到一个单元。
4. **确定性平局**：得分相同的单元先由 `splitmix64(seed) ^ splitmix64(caseId)` 比较，再由单元序号兜底。因此平局结果只依赖种子，不依赖线程调度或 `HashMap` 迭代顺序。

### 2.2 跨单元依赖

依赖与互斥在**执行期全局生效**，不要求依赖双方在同一分片；当放在一起不影响负载时规划器倾向于把依赖链放同一单元，但这只是优化。用例只有在其全部依赖已提交为 PASSED 时才会开始执行。

---

## 3. 调度、配额与互斥语义

`ResourceManager` 同时管理三层约束：

1. **全局并发上限** `maxConcurrency`（公平信号量）；
2. **资源标签配额** `resourceQuotas`（每个标签一个公平信号量，未声明的标签默认取全局上限）；
3. **互斥组**：内部映射为配额 1 的标签。

- 所有等待都使用**公平 FIFO** 信号量排队，超出配额即等待，不会超发；
- 为避免多标签死锁，标签始终按**字典序**加锁（互斥组使用 `mutex::<组名>` 的内部命名空间），反向释放；
- 获取过程可被取消/超时中断，中断时已获取的许可会全部回滚，**不泄漏配额**。

被阻塞的用例不占用任何资源许可；前置未完成的用例在调度器上等待（条件变量），而非忙等。

---

## 4. 重试、不稳定识别与隔离

### 4.1 唯一结论与不掩盖失败

每个用例在一次运行中只会被提交**一个**最终状态：`PASSED / FAILED / SKIPPED`（CAS 发布，见 `ResultStore`）。

- **确定性失败（`AssertionError` 或显式 `DETERMINISTIC`）立即 FAILED，不重试**——重试策略永远无法掩盖它；
- `RETRIABLE` 失败、单用例超时、中断按预算重试；预算耗尽 ⇒ FAILED；
- 每次尝试（attempt）都以不可变 `AttemptRecord` 记录：尝试序号、被分配单元、实际执行单元（崩溃再分发后可能不同）、状态、失败类型、耗时、中断/崩溃标记。

### 4.2 不稳定识别（FlakyRegistry）

一次运行中，只要某用例出现过 `RETRIABLE` 失败（无论最终是否转 PASS），就累加一个“抖动观测值”；累计达到 `flakyThreshold` 即认定为不稳定。

- 注册中心默认内存态，可选 `FlakyRegistry.fromFile(path)` 文件持久化；
- 持久化使用**临时文件 + `ATOMIC_MOVE`**（不支持原子移动的文件系统回退为普通 move），中断时要么保留旧文件、要么是完整新文件，不会出现半写入损坏；
- 运行中途新变不稳定的用例，当次即可获得 `flakyMaxRetries` 的更大重试预算。

### 4.3 隔离策略 `IsolationStrategy`

| 策略 | 行为 |
| --- | --- |
| `NONE` | 不做历史隔离；不稳定用例在常规通道重试。 |
| `QUARANTINE_AFTER` | 已知不稳定用例在**所有稳定用例落定后**进入隔离通道执行；隔离通道使用独立配额 `quarantineConcurrency`（默认 1，串行），不与稳定用例抢资源。结果中 `quarantined=true`。 |
| `SKIP_QUARANTINED` | 已知不稳定用例在运行开始前直接标记 SKIPPED（`SkipReason.QUARANTINED`），一次都不执行。 |

---

## 5. 崩溃、取消与超时的失败治理

### 5.1 执行单元崩溃

`UnitCrashException` 表示工作单元发生“硬崩溃”（线程死亡）：

- 在飞用例的当前尝试记录为 `CRASHED`（`crashed=true`，历史尝试保留不丢）；
- 死亡单元队列中剩余的用例与在飞用例，按“存活单元中队列最短、序号最小”的**确定性规则再分发**；原始分配单元在审计中仍可追溯（`assignedUnitId` vs `executedUnitId`）；
- 若没有存活单元，剩余用例一律 SKIPPED（`UNIT_CRASHED`）；
- **未完成用例绝不会被计为 PASSED**；已提交的结果不会丢失。

### 5.2 单用例超时

`defaultTimeoutMillis`（0 = 不限）通过共享定时器到期中断工作线程实现；超时尝试状态为 `TIMEOUT`、计为 `RETRIABLE`，并在下次尝试前清除中断位。忽略中断的用例体属于协作式放弃，超时精度为“尽力而为”。

### 5.3 运行取消 / 运行级超时

- `TestOrchestrator.cancel()` 或 `runTimeoutMillis` 到期会把运行置为 aborted，并中断所有存活工作线程；
- 已完成并提交的结果**原样保留**；
- 仍在飞或尚未开始的用例统一为 **SKIPPED**：取消 ⇒ `CANCELLED`，运行超时 ⇒ `RUN_TIMEOUT`；不会出现 FAILED 与“隐式通过”。

### 5.4 无部分写入

- `ResultStore`：尝试先追加、最终状态与完整尝试列表一次性 CAS 发布，外部永远看不到“有最终状态但缺尝试”的中间态；
- 审计文件与 flaky 注册中心文件均为临时文件 + 原子移动；
- 运行收尾做一次确定性 sweep：理论上遗漏的未提交用例按原因兜底 SKIPPED，保证 pass/fail/skip 三集完整覆盖输入集合。

---

## 6. 结果归并（Aggregation）与审计语义

`RunResult` 是一次运行的不可变归并结果：

- `passedIds() / failedIds() / skippedIds()` 三个集合**两两不交，且并集 = 全部输入用例 id**；
- `caseResults` 始终按用例 id 排序 ⇒ 相同输入下输出字节级确定，可直接做跨次 diff；
- 每个 `CaseResult` 都能回答“属于哪个计划单元、最终在哪个单元执行、一共尝试了几次、每次的细节、是否不稳定/隔离”，即**按用例可追溯归属与尝试次数**；
- `cancelled / timedOut` 标记运行的终止方式。

`runWithAudit(...)` 额外输出 JSON 审计报告（`AuditReporter`，Jackson，键序确定），包含：runId、种子、单元数、取消/超时标记、完整分片计划、每个用例的最终状态与逐次尝试。写入是原子的，适合作为 CI 归档与跨运行比较输入。

---

## 7. 确定性与可复现性说明

- **分片可复现**：纯函数 + 种子平局（第 2 节）；
- **汇总可复现**：三集只取决于用例声明、执行体行为、配置与种子；归并结果按 id 排序；
- **不稳定用例的注意点**：`FlakyRegistry` 的历史观测是跨运行累积状态。需要严格字节级复现一次运行时，应使用全新的内存注册中心或固定其文件内容；注册中心的存在正是为了让“隔离决策”随历史演进而可追溯，而不是让每次运行互相不可解释。

---

## 8. 包结构

| 包 | 职责 |
| --- | --- |
| `orchestration.model` | `TestCase`、状态枚举、`AttemptRecord/CaseResult/RunResult` |
| `orchestration.shard` | 拓扑校验与确定性负载均衡分片 |
| `orchestration.resource` | 全局/标签/互斥配额与公平排队 |
| `orchestration.flaky` | 不稳定识别与原子持久化注册中心 |
| `orchestration.executor` | 执行体接口、上下文、失败分类、失败/崩溃异常 |
| `orchestration.run` | 调度引擎：worker、重试、隔离通道、崩溃恢复、超时/取消、结果存储 |
| `orchestration.audit` | 审计报告模型与原子 JSON 落盘 |
| `orchestration.TestOrchestrator` | 对外门面：配置、规划、执行、取消、审计 |

---

## 9. 测试覆盖

`src/test/.../orchestration` 下提供 30+ 个自动化用例：

- `ShardPlannerTest`：可复现、种子敏感、全覆盖、负载均衡、未知依赖/依赖环、拓扑顺序；
- `DependencyTest`：前置失败/跳过的后继明确 SKIPPED（非 FAILED）、传递跳过、跨单元依赖链；
- `MutexConcurrencyTest`：同互斥组不重叠、无关用例仍并发；
- `ResourceManagerTest`：全局上限、标签配额、互斥组配额 1、排队取消不泄漏许可；
- `FlakyRetryTest`：确定性失败不重试、抖动重试转通过、预算耗尽唯一 FAILED、两种隔离策略、重试不掩盖确定性失败；
- `CrashTimeoutTest`：崩溃记录 CRASHED、再分发到存活单元、尝试历史不丢、单用例超时重试、运行超时保留已完成并跳过未完成、单元/尝试可追溯；
- `CancelTest`：外部取消保留已完成、在飞与未开始均 SKIPPED；
- `AuditAndDeterminismTest`：三集确定且不交、审计 JSON 原子落盘含计划与尝试、分片跨实例可复现、flaky 文件重建。
