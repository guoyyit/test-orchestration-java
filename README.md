# test-orchestration-java

本地测试执行编排能力：把大量测试用例分配到多个执行单元（executor unit）并发执行，
支持依赖约束、资源隔离、失败治理与结果审计。核心代码位于
`com.github.guoyyit.testorchestrationjava.orchestration`，纯 Java 实现，不依赖 Spring 运行时。

## 快速上手

```java
List<TestCaseSpec> cases = List.of(
    new TestCaseSpec("setup",    Set.of(),        Set.of(),        Set.of("db"), Duration.ofMillis(100), false),
    new TestCaseSpec("api-test", Set.of("setup"), Set.of(),        Set.of("db"), Duration.ofMillis(500), true),
    new TestCaseSpec("ui-test",  Set.of("setup"), Set.of("screen"), Set.of(),    Duration.ofMillis(900), false));

OrchestrationConfig config = new OrchestrationConfig(
        4,                                          // concurrency: 并发上限（执行单元数）
        List.of(new ResourceQuota("db", 2)),        // quotas: 资源标签配额
        new RetryPolicy(3, true),                   // retryPolicy: 不稳定用例最多 3 次尝试
        Duration.ofSeconds(30),                     // caseTimeout: 单次尝试超时
        42L);                                       // shardSeed: 分片随机种子

RunSummary summary = new Orchestrator(config).run(cases, caseId -> {
    // 执行真实测试体；抛出异常即视为本次尝试失败
});
```

## 用例声明（TestCaseSpec）

| 字段 | 语义 |
|---|---|
| `dependsOn` | 前置依赖。所有依赖通过后才会调度；任一依赖失败/被跳过，本用例标记为 **SKIPPED**（不是 FAILED），并沿依赖链向下传播。 |
| `mutexGroups` | 互斥组。同组用例绝不并发执行。 |
| `resourceTags` | 资源标签。执行前必须取得每个标签的配额许可，否则排队等待。 |
| `historicalDuration` | 历史耗时，分片负载均衡的输入。 |
| `flaky` | 不稳定标记。只有 flaky 用例才会按 `RetryPolicy` 重试。 |

依赖环、未知依赖、重复用例 id 会在运行前直接抛出 `IllegalArgumentException`。

## 分片语义（ShardPlanner）

`ShardPlanner(seed).plan(cases, shardCount)` 采用 **LPT（最长耗时优先）贪心**：

1. 用例按历史耗时降序排列；耗时相同按 `(seed, caseId)` 的稳定哈希排序，
   因此**相同输入 + 相同种子必然得到相同分片**，且与输入列表顺序无关。
2. 依次把用例放入当前代价最低的分片：代价 = 已积累耗时 + 每个同标签用例的虚拟惩罚，
   使运行时长与同类资源标签（如 `db`）在用例间双重均衡。

分片结果 `ShardPlan` 携带每个分片的用例列表与预估耗时，可用于静态分批；
各分片再分别交给 `Orchestrator` 执行。

## 调度与资源语义（Orchestrator）

- **并发上限**：执行单元数 = `concurrency`，任何时刻运行中的用例不超过该上限。
- **资源配额**：每个资源标签一个公平信号量。用例按全局排序顺序一次性获取全部标签许可，
  避免死锁；配额不足时排队等待，**不会超发**。
- **超时**：每次尝试受 `caseTimeout` 约束，超时记为一次失败尝试并中断执行线程。
- **取消**：`cancel()` 停止调度并中断在途尝试。已提交（committed）的结果完整保留；
  未完成的用例不会产生部分写入，最终计入 SKIPPED，**绝不会计为通过**。
- **崩溃治理**：用例体抛出任何 `Throwable`（包括 `Error`）只记为该用例的失败尝试，
  不影响其他用例与整体运行。

## 重试与隔离语义（RetryPolicy）

- 仅 `flaky=true` 的用例可重试，最多 `maxAttempts` 次尝试；
  确定性用例永远只跑一次——**重试不会掩盖确定性失败**。
- 任一尝试通过则最终结论为 PASSED；全部尝试失败为 FAILED。
- 每次尝试都记录 `AttemptRecord`（尝试序号、执行单元、起止时间、状态、错误），
  靠重试才通过的 flaky 用例可通过 `attempts > 1` 被追溯、隔离与治理。

## 归并与审计语义（ResultStore / RunSummary）

- 每个用例的最终结论**唯一且原子提交**：要么完整可见（含全部尝试历史），要么不可见；
  重复提交会被拒绝，崩溃不会留下部分写入。
- `RunSummary` 的 passed / failed / skipped 三个集合**两两不相交、按字典序排序、
  并集等于全部提交用例**——同一次运行的结论是确定的。
- 未提交结果的用例一律计入 skipped，不会计为通过。
- 每个用例可通过 `summary.results()` 追溯归属的执行单元（`executorId`）与尝试次数（`attempts`）。

## 测试

```bash
mvn test
```

覆盖：依赖冲突（环/未知依赖/失败传播跳过）、资源互斥、并发上限、资源配额排队、
不稳定重试与审计、超时、崩溃、取消，以及分片可复现性与负载均衡。
