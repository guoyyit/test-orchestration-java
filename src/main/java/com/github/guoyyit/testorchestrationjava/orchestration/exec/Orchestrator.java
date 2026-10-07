package com.github.guoyyit.testorchestrationjava.orchestration.exec;

import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptRecord;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunSummary;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCaseSpec;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.result.ResultStore;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Central scheduling engine.
 *
 * <p>Semantics:</p>
 * <ul>
 *   <li>A case runs only after <b>all</b> its dependencies passed; a failed or
 *       skipped dependency marks the whole dependent subtree SKIPPED (never FAILED).</li>
 *   <li>Cases sharing a mutex group never run concurrently.</li>
 *   <li>At most {@code concurrency} cases run at once; resource tags are guarded
 *       by fair semaphores so quotas are never oversubscribed — excess cases queue.</li>
 *   <li>Only cases declared {@code flaky} are retried (bounded by
 *       {@link RetryPolicy}); deterministic cases are never re-run, so retries
 *       cannot mask a deterministic failure. Every attempt is recorded.</li>
 *   <li>Each attempt is bounded by {@code caseTimeout}; timeout/interruption of an
 *       attempt is a failed attempt, never a silent pass.</li>
 *   <li>{@link #cancel()} stops scheduling and interrupts in-flight attempts;
 *       already committed results are kept, uncommitted cases end up SKIPPED.</li>
 * </ul>
 *
 * <p>One Orchestrator instance executes one run.</p>
 */
public class Orchestrator {

    private final OrchestrationConfig config;
    private final List<ExecutorUnit> units;
    private final BlockingQueue<ExecutorUnit> idleUnits;

    private volatile boolean cancelled;
    private ExecutorService taskPool;
    private ExecutorService attemptPool;

    public Orchestrator(OrchestrationConfig config) {
        this.config = config;
        this.units = new ArrayList<>();
        for (int i = 1; i <= config.concurrency(); i++) {
            this.units.add(new ExecutorUnit("executor-" + i));
        }
        this.idleUnits = new ArrayBlockingQueue<>(config.concurrency(), true, this.units);
    }

    /** Request cancellation: stop scheduling, interrupt in-flight attempts. */
    public void cancel() {
        cancelled = true;
        if (taskPool != null) taskPool.shutdownNow();
        if (attemptPool != null) attemptPool.shutdownNow();
    }

    public RunSummary run(List<TestCaseSpec> cases, TestExecutor executor) {
        String runId = "run-" + UUID.randomUUID();
        Map<String, TestCaseSpec> specs = indexAndValidate(cases);
        Map<String, Set<String>> dependents = buildDependents(specs);
        Map<String, Integer> indegree = initialIndegrees(specs);

        ResultStore store = new ResultStore();
        Map<String, Semaphore> quotas = quotaSemaphores();
        Map<String, String> mutexOwner = new HashMap<>();
        Map<String, TestStatus> finalStatus = new HashMap<>();
        Set<String> scheduled = new HashSet<>();
        BlockingQueue<Completion> completions = new LinkedBlockingQueue<>();

        taskPool = Executors.newCachedThreadPool(daemonFactory("case-task"));
        attemptPool = Executors.newCachedThreadPool(daemonFactory("attempt"));
        try {
            int total = specs.size();
            int finished = 0;
            while (finished < total && !cancelled) {
                submitReady(specs, indegree, finalStatus, scheduled, mutexOwner,
                        quotas, store, completions, executor);
                Completion c = completions.poll(50, TimeUnit.MILLISECONDS);
                if (c == null) {
                    continue;
                }
                releaseMutex(specs.get(c.caseId()), mutexOwner);
                if (c.result() == null) {
                    continue; // aborted by cancellation: no commit, no conclusion
                }
                finished++;
                finalStatus.put(c.caseId(), c.result().status());
                if (c.result().status() == TestStatus.PASSED) {
                    for (String dep : dependents.get(c.caseId())) {
                        indegree.merge(dep, -1, Integer::sum);
                    }
                } else {
                    finished += propagateSkip(c.caseId(), dependents, finalStatus, store);
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            cancelled = true;
        } finally {
            shutdownPools();
        }
        return store.summarize(runId, List.copyOf(specs.keySet()));
    }

    // ---- scheduling -----------------------------------------------------

    private void submitReady(Map<String, TestCaseSpec> specs,
                             Map<String, Integer> indegree,
                             Map<String, TestStatus> finalStatus,
                             Set<String> scheduled,
                             Map<String, String> mutexOwner,
                             Map<String, Semaphore> quotas,
                             ResultStore store,
                             BlockingQueue<Completion> completions,
                             TestExecutor executor) {
        List<String> ready = new ArrayList<>();
        for (String id : specs.keySet()) {
            if (!finalStatus.containsKey(id) && !scheduled.contains(id) && indegree.get(id) == 0) {
                ready.add(id);
            }
        }
        Collections.sort(ready); // deterministic dispatch order
        for (String id : ready) {
            TestCaseSpec spec = specs.get(id);
            if (!mutexFree(spec, mutexOwner)) {
                continue; // blocked by a running case in the same mutex group
            }
            ExecutorUnit unit = idleUnits.poll();
            if (unit == null) {
                break; // all units busy: queue until a completion frees one
            }
            scheduled.add(id);
            lockMutex(spec, mutexOwner, id);
            taskPool.submit(() -> runCase(spec, executor, unit, quotas, store, completions));
        }
    }

    private void runCase(TestCaseSpec spec, TestExecutor executor, ExecutorUnit unit,
                         Map<String, Semaphore> quotas, ResultStore store,
                         BlockingQueue<Completion> completions) {
        List<Semaphore> acquired = new ArrayList<>();
        List<AttemptRecord> history = new ArrayList<>();
        boolean committed = false;
        try {
            // acquire all quotas in globally sorted order: no deadlock, no oversubscription
            for (String tag : spec.resourceTags().stream().sorted().toList()) {
                Semaphore s = quotas.get(tag);
                if (s != null) {
                    s.acquire();
                    acquired.add(s);
                }
            }
            int maxAttempts = spec.flaky() ? config.retryPolicy().maxAttempts() : 1;
            TestStatus conclusion = TestStatus.FAILED;
            String error = null;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                if (cancelled) {
                    return; // interrupted run: leave no partial write
                }
                Instant start = Instant.now();
                AttemptOutcome outcome = runAttempt(spec.id(), executor);
                Instant end = Instant.now();
                history.add(new AttemptRecord(attempt, unit.id(), start, end, outcome.status(), outcome.error()));
                if (outcome.status() == TestStatus.PASSED) {
                    conclusion = TestStatus.PASSED;
                    error = null;
                    break;
                }
                error = outcome.error();
            }
            store.commit(new TestResult(spec.id(), conclusion, history.size(), unit.id(),
                    history, error));
            committed = true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt(); // cancelled: leave no partial write
        } finally {
            for (Semaphore s : acquired) {
                s.release();
            }
            idleUnits.offer(unit);
            TestResult committed1 = committed ? store.find(spec.id()).orElse(null) : null;
            completions.offer(new Completion(spec.id(), committed1));
        }
    }

    private AttemptOutcome runAttempt(String caseId, TestExecutor executor) throws InterruptedException {
        Future<?> future = attemptPool.submit(() -> {
            executor.execute(caseId);
            return null;
        });
        try {
            future.get(config.caseTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return new AttemptOutcome(TestStatus.PASSED, null);
        } catch (TimeoutException te) {
            future.cancel(true);
            return new AttemptOutcome(TestStatus.FAILED,
                    "timeout after " + config.caseTimeout().toMillis() + "ms");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause() == null ? ee : ee.getCause();
            return new AttemptOutcome(TestStatus.FAILED,
                    cause.getClass().getSimpleName() + ": " + String.valueOf(cause.getMessage()));
        } catch (InterruptedException ie) {
            future.cancel(true);
            throw ie;
        }
    }

    // ---- dependency graph ----------------------------------------------

    private Map<String, TestCaseSpec> indexAndValidate(List<TestCaseSpec> cases) {
        Map<String, TestCaseSpec> specs = new LinkedHashMap<>();
        for (TestCaseSpec c : cases) {
            if (specs.put(c.id(), c) != null) {
                throw new IllegalArgumentException("duplicate case id: " + c.id());
            }
        }
        for (TestCaseSpec c : cases) {
            for (String dep : c.dependsOn()) {
                if (!specs.containsKey(dep)) {
                    throw new IllegalArgumentException(
                            "case " + c.id() + " depends on unknown case " + dep);
                }
            }
        }
        // cycle check (Kahn)
        Map<String, Integer> indeg = initialIndegrees(specs);
        Map<String, Set<String>> dependents = buildDependents(specs);
        Deque<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> e : indeg.entrySet()) {
            if (e.getValue() == 0) queue.add(e.getKey());
        }
        int processed = 0;
        while (!queue.isEmpty()) {
            String id = queue.poll();
            processed++;
            for (String dep : dependents.get(id)) {
                if (indeg.merge(dep, -1, Integer::sum) == 0) {
                    queue.add(dep);
                }
            }
        }
        if (processed != specs.size()) {
            throw new IllegalArgumentException("dependency cycle detected");
        }
        return specs;
    }

    private Map<String, Set<String>> buildDependents(Map<String, TestCaseSpec> specs) {
        Map<String, Set<String>> dependents = new HashMap<>();
        for (String id : specs.keySet()) {
            dependents.put(id, new HashSet<>());
        }
        for (TestCaseSpec c : specs.values()) {
            for (String dep : c.dependsOn()) {
                dependents.get(dep).add(c.id());
            }
        }
        return dependents;
    }

    private Map<String, Integer> initialIndegrees(Map<String, TestCaseSpec> specs) {
        Map<String, Integer> indegree = new HashMap<>();
        for (TestCaseSpec c : specs.values()) {
            indegree.put(c.id(), c.dependsOn().size());
        }
        return indegree;
    }

    /** Mark the whole dependent subtree of a failed/skipped case as SKIPPED. */
    private int propagateSkip(String failedId, Map<String, Set<String>> dependents,
                              Map<String, TestStatus> finalStatus, ResultStore store) {
        int count = 0;
        Deque<String> stack = new ArrayDeque<>();
        stack.push(failedId);
        while (!stack.isEmpty()) {
            String current = stack.pop();
            for (String dependent : dependents.get(current)) {
                if (finalStatus.containsKey(dependent)) {
                    continue;
                }
                finalStatus.put(dependent, TestStatus.SKIPPED);
                store.commit(new TestResult(dependent, TestStatus.SKIPPED, 0, null,
                        List.of(), "dependency " + current + " did not pass"));
                count++;
                stack.push(dependent);
            }
        }
        return count;
    }

    // ---- mutex & quotas --------------------------------------------------

    private boolean mutexFree(TestCaseSpec spec, Map<String, String> mutexOwner) {
        for (String group : spec.mutexGroups()) {
            if (mutexOwner.containsKey(group)) {
                return false;
            }
        }
        return true;
    }

    private void lockMutex(TestCaseSpec spec, Map<String, String> mutexOwner, String caseId) {
        for (String group : spec.mutexGroups()) {
            mutexOwner.put(group, caseId);
        }
    }

    private void releaseMutex(TestCaseSpec spec, Map<String, String> mutexOwner) {
        for (String group : spec.mutexGroups()) {
            mutexOwner.remove(group);
        }
    }

    private Map<String, Semaphore> quotaSemaphores() {
        Map<String, Semaphore> quotas = new HashMap<>();
        for (var q : config.quotas()) {
            quotas.put(q.tag(), new Semaphore(q.capacity(), true));
        }
        return quotas;
    }

    // ---- lifecycle -------------------------------------------------------

    private void shutdownPools() {
        if (taskPool != null) taskPool.shutdownNow();
        if (attemptPool != null) attemptPool.shutdownNow();
        try {
            if (taskPool != null) taskPool.awaitTermination(5, TimeUnit.SECONDS);
            if (attemptPool != null) attemptPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private record Completion(String caseId, TestResult result) {
    }

    private record AttemptOutcome(TestStatus status, String error) {
    }
}
