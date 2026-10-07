package com.github.guoyyit.testorchestrationjava.orchestration.run;

import com.github.guoyyit.testorchestrationjava.orchestration.OrchestrationConfig;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.ExecutionContext;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.FailureClassifier;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestCaseView;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestExecutable;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestExecutor;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.UnitCrashException;
import com.github.guoyyit.testorchestrationjava.orchestration.flaky.FlakyRegistry;
import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptRecord;
import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.model.FailureType;
import com.github.guoyyit.testorchestrationjava.orchestration.model.IsolationStrategy;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.SkipReason;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import com.github.guoyyit.testorchestrationjava.orchestration.resource.ResourceLease;
import com.github.guoyyit.testorchestrationjava.orchestration.resource.ResourceManager;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardAssignment;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlan;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlanner;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One concrete execution of a shard plan.
 *
 * <p>Each execution unit owns one worker thread and a FIFO of case ids; workers
 * pull from their FIFO under a single scheduler lock, skip when a dependency
 * failed, wait (without consuming resources) when dependencies are running or
 * quotas are full, and execute attempts while holding global/per-label/mutex
 * permits. Attempts support per-case timeout, retries (deterministic failures
 * are never retried), mid-run flaky detection and two-phase quarantine, and
 * crash recovery: when a unit dies, its in-flight attempt is recorded CRASHED
 * and its work is redistributed to surviving units. Final statuses are
 * committed exactly once; on cancellation/run-timeout every unfinished case is
 * deterministically SKIPPED (never PASSED).</p>
 */
public final class OrchestrationRun {

    private static final FailureClassifier CLASSIFIER = FailureClassifier.DEFAULT;

    private final String runId;
    private final List<TestCase> cases;
    private final Map<String, TestExecutable> executables;
    private final OrchestrationConfig config;
    private final FlakyRegistry flakyRegistry;
    private final ShardPlan plan;

    private final ResultStore store = new ResultStore();
    private final ResourceManager stableResources;
    private final ResourceManager quarantineResources;
    private final Map<String, TestCase> byId = new LinkedHashMap<>();
    private final Map<String, String> plannedUnit = new HashMap<>();
    private final Set<String> quarantined = new HashSet<>();

    private final Object lock = new Object();
    private final Deque<String>[] unitQueues;
    private final Set<String>[] unitOwners;
    private final Set<String> inFlight = new HashSet<>();
    private final String[] inflightByUnit;
    private final Set<Integer> deadUnits = new HashSet<>();
    private final Deque<String> quarantinePending = new ArrayDeque<>();
    private int aliveUnits;
    private boolean stablePhaseOpen;
    private boolean aborted;
    private boolean cancelled;
    private boolean timedOut;

    private ScheduledExecutorService timer;
    private Thread[] workerThreads;
    private final AtomicBoolean cancelRequested = new AtomicBoolean();

    @SuppressWarnings("unchecked")
    public OrchestrationRun(String runId, List<TestCase> cases, TestExecutor executor,
                            OrchestrationConfig config, FlakyRegistry flakyRegistry, ShardPlan plan) {
        this.runId = runId;
        this.cases = List.copyOf(cases);
        this.config = config;
        this.flakyRegistry = flakyRegistry == null ? new FlakyRegistry() : flakyRegistry;
        this.plan = plan;
        Map<String, TestExecutable> resolved = executor.resolve(this.cases);
        this.executables = resolved == null ? Map.of() : resolved;
        for (TestCase tc : this.cases) {
            byId.put(tc.id(), tc);
        }
        this.stableResources = new ResourceManager(config.maxConcurrency(), config.resourceQuotas());
        int qConcurrency = Math.max(1, Math.min(config.quarantineConcurrency(), config.maxConcurrency()));
        this.quarantineResources = new ResourceManager(qConcurrency, config.resourceQuotas());

        int n = config.unitCount();
        this.unitQueues = new Deque[n];
        this.unitOwners = new Set[n];
        this.inflightByUnit = new String[n];
        this.aliveUnits = n;
        for (int i = 0; i < n; i++) {
            unitQueues[i] = new ArrayDeque<>();
            unitOwners[i] = new HashSet<>();
        }
        for (ShardAssignment shard : plan.shards()) {
            int idx = Integer.parseInt(shard.unitId().substring("unit-".length()));
            for (String id : shard.caseIds()) {
                plannedUnit.put(id, shard.unitId());
                unitOwners[idx].add(id);
            }
        }

        // Pre-run isolation decisions come solely from the historical registry.
        boolean quarantineLane = config.isolationStrategy() == IsolationStrategy.QUARANTINE_AFTER;
        this.stablePhaseOpen = quarantineLane;
        List<String> knownFlaky = new ArrayList<>(
                this.flakyRegistry.knownFlakyCases(config.flakyThreshold()));
        knownFlaky.sort(String::compareTo);
        for (String id : knownFlaky) {
            if (!byId.containsKey(id)) {
                continue;
            }
            quarantined.add(id);
            if (config.isolationStrategy() == IsolationStrategy.SKIP_QUARANTINED) {
                // finalized before any worker starts
            } else if (quarantineLane) {
                quarantinePending.addLast(id);
            }
        }
        store.register(byId.keySet());

        // Stable cases are enqueued in their planned shard order; quarantine cases wait.
        for (ShardAssignment shard : plan.shards()) {
            int idx = Integer.parseInt(shard.unitId().substring("unit-".length()));
            for (String id : shard.caseIds()) {
                if (quarantinePending.contains(id)
                        || config.isolationStrategy() == IsolationStrategy.SKIP_QUARANTINED
                            && quarantined.contains(id)) {
                    continue;
                }
                unitQueues[idx].addLast(id);
            }
        }
    }

    /** Runs to completion, cancellation, or run timeout; always returns a full result. */
    public RunResult execute() {
        // SKIP_QUARANTINED: hold out known unstable cases up front.
        if (config.isolationStrategy() == IsolationStrategy.SKIP_QUARANTINED) {
            for (String id : quarantined) {
                store.finalizeCase(id, CaseStatus.SKIPPED, SkipReason.QUARANTINED,
                        plannedUnit.get(id), null, true, true);
            }
            quarantinePending.clear();
        }

        timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "orchestration-timer-" + runId);
            t.setDaemon(true);
            return t;
        });
        ScheduledFuture<?> runTimeoutTask = null;
        if (config.runTimeoutMillis() > 0) {
            runTimeoutTask = timer.schedule(this::timeoutRun, config.runTimeoutMillis(),
                    TimeUnit.MILLISECONDS);
        }

        workerThreads = new Thread[config.unitCount()];
        for (int i = 0; i < config.unitCount(); i++) {
            final int idx = i;
            Thread w = new Thread(() -> workerLoop(idx), "orchestration-" + ShardPlanner.unitId(i));
            w.setDaemon(false);
            w.setUncaughtExceptionHandler((t, e) -> onUnitDied(idx, e));
            workerThreads[i] = w;
        }
        for (Thread w : workerThreads) {
            w.start();
        }

        synchronized (lock) {
            while (!allDoneLocked()) {
                try {
                    lock.wait(1_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    requestAbort(true, false);
                }
            }
        }

        if (runTimeoutTask != null) {
            runTimeoutTask.cancel(false);
        }
        timer.shutdownNow();
        for (Thread w : workerThreads) {
            try {
                w.join(10_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Deterministic end-of-run sweep: any case with no committed result is
        // SKIPPED, never implicitly passed.
        List<String> leftover = new ArrayList<>();
        for (String id : byId.keySet()) {
            if (!store.isFinalized(id)) {
                leftover.add(id);
            }
        }
        leftover.sort(String::compareTo);
        SkipReason reason = aborted ? (timedOut ? SkipReason.RUN_TIMEOUT : SkipReason.CANCELLED)
                : SkipReason.UNIT_CRASHED;
        for (String id : leftover) {
            store.finalizeCase(id, CaseStatus.SKIPPED, reason,
                    plannedUnit.get(id), null, false, quarantined.contains(id));
        }
        return new RunResult(runId, store.snapshot(), cancelled, timedOut);
    }

    /** Requests cooperative cancellation; in-flight attempts are interrupted. */
    public void cancel() {
        cancelRequested.set(true);
        requestAbort(true, false);
    }

    private void timeoutRun() {
        requestAbort(false, true);
    }

    private void requestAbort(boolean userCancelled, boolean runTimeout) {
        List<Thread> toInterrupt = new ArrayList<>();
        synchronized (lock) {
            if (aborted) {
                return;
            }
            aborted = true;
            cancelled = userCancelled || cancelled;
            timedOut = runTimeout || timedOut;
            lock.notifyAll();
        }
        // interrupt outside the scheduler lock
        for (int i = 0; i < workerThreads.length; i++) {
            Thread t = workerThreads[i];
            if (t != null && t.isAlive()) {
                toInterrupt.add(t);
            }
        }
        for (Thread t : toInterrupt) {
            t.interrupt();
        }
    }

    private boolean isAborted() {
        synchronized (lock) {
            return aborted;
        }
    }

    // ------------------------------------------------------------------ worker

    private void workerLoop(int unit) {
        try {
            while (true) {
                String caseId;
                synchronized (lock) {
                    while (true) {
                        if (aborted) {
                            skipQueueOnAbort(unit);
                            return;
                        }
                        String head = unitQueues[unit].peek();
                        if (head == null) {
                            if (moreWorkPossibleLocked(unit)) {
                                try {
                                    lock.wait(200L);
                                } catch (InterruptedException e) {
                                    if (aborted) {
                                        skipQueueOnAbort(unit);
                                        return;
                                    }
                                    Thread.currentThread().interrupt();
                                }
                                continue;
                            }
                            return;
                        }
                        if (inFlight.contains(head) || anyDependencyRunning(head)) {
                            try {
                                lock.wait(200L);
                            } catch (InterruptedException e) {
                                if (aborted) {
                                    skipQueueOnAbort(unit);
                                    return;
                                }
                                Thread.currentThread().interrupt();
                            }
                            continue;
                        }
                        if (anyDependencyFailedOrSkipped(head)) {
                            unitQueues[unit].poll();
                            commitSkipped(head, SkipReason.DEPENDENCY_FAILED);
                            afterCaseCommitted();
                            continue;
                        }
                        unitQueues[unit].poll();
                        inFlight.add(head);
                        inflightByUnit[unit] = head;
                        caseId = head;
                        break;
                    }
                }
                executeCase(unit, caseId);
            }
        } finally {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    private boolean moreWorkPossibleLocked(int unit) {
        if (!unitQueues[unit].isEmpty()) {
            return true;
        }
        if (stablePhaseOpen && !quarantinePending.isEmpty()) {
            return aliveUnits > 0 || !quarantinePending.isEmpty();
        }
        // redistributions may arrive from other (still alive) units
        for (int i = 0; i < unitQueues.length; i++) {
            if (deadUnits.contains(i)) {
                continue;
            }
            if (i != unit && !unitQueues[i].isEmpty()) {
                return true;
            }
        }
        return !inFlight.isEmpty();
    }

    private boolean anyDependencyRunning(String caseId) {
        TestCase tc = byId.get(caseId);
        for (String dep : tc.dependsOn()) {
            if (!store.isFinalized(dep)) {
                return true;
            }
        }
        return false;
    }

    private boolean anyDependencyFailedOrSkipped(String caseId) {
        TestCase tc = byId.get(caseId);
        for (String dep : tc.dependsOn()) {
            var st = store.statusOf(dep);
            if (st.isPresent() && st.get() != CaseStatus.PASSED) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------ attempt

    private void executeCase(int unit, String caseId) {
        boolean isQuarantine = quarantined.contains(caseId);
        int stableBudget = 1 + Math.max(0, config.maxRetries());
        int quarantineBudget = 1 + Math.max(0, Math.max(config.maxRetries(), config.flakyMaxRetries()));
        int budget = isQuarantine ? quarantineBudget : stableBudget;

        TestExecutable executable = executables.get(caseId);
        if (executable == null) {
            recordAttempt(caseId, unit, unit, AttemptStatus.FAILED,
                    FailureType.DETERMINISTIC, "no executable registered for case: " + caseId,
                    0, false, false);
            finishCase(caseId, unit, CaseStatus.FAILED, null, isQuarantine, false);
            return;
        }

        boolean sawRetriable = false;
        int attempt = 1;
        while (true) {
            AttemptOutcome outcome = runAttempt(unit, caseId, executable, attempt, isQuarantine);
            if (outcome.crashed) {
                return; // this worker is dying; onUnitDied records/redistributes
            }
            // A per-case timeout leaves the worker thread interrupted; clear
            // the flag so a retry starts clean (the timeout task is already cancelled).
            if (outcome.interrupted() && !isAborted()) {
                Thread.interrupted();
            }
            recordAttempt(caseId, unit, outcome.executedUnit, outcome.status,
                    outcome.failureType, outcome.message, outcome.durationMillis,
                    outcome.interrupted, false);

            if (isAborted() && (outcome.status == AttemptStatus.INTERRUPTED
                    || outcome.status == AttemptStatus.TIMEOUT)) {
                // run-level cancel/timeout: unfinished case is SKIPPED, not FAILED
                finishCase(caseId, outcome.executedUnit, CaseStatus.SKIPPED,
                        timedOut ? SkipReason.RUN_TIMEOUT : SkipReason.CANCELLED,
                        isQuarantine, sawRetriable);
                return;
            }

            if (outcome.status == AttemptStatus.PASSED) {
                boolean knownFlakyBefore =
                        flakyRegistry.isKnownFlaky(caseId, config.flakyThreshold());
                flakyRegistry.recordRun(caseId, sawRetriable, true, config.flakyThreshold());
                finishCase(caseId, outcome.executedUnit, CaseStatus.PASSED, null,
                        isQuarantine, knownFlakyBefore || sawRetriable);
                return;
            }

            if (outcome.status == AttemptStatus.TIMEOUT
                    || outcome.status == AttemptStatus.INTERRUPTED
                    || outcome.failureType == FailureType.RETRIABLE) {
                sawRetriable = true;
            } else {
                // DETERMINISTIC: never retried, never masked by retry policy
                flakyRegistry.recordRun(caseId, false, false, config.flakyThreshold());
                finishCase(caseId, outcome.executedUnit, CaseStatus.FAILED, null,
                        isQuarantine, false);
                return;
            }

            int effectiveBudget = budget;
            // a case that becomes unstable mid-run gets the flaky retry budget
            if (!isQuarantine
                    && config.isolationStrategy() != IsolationStrategy.NONE
                    && flakyRegistry.isKnownFlaky(caseId, config.flakyThreshold())
                    && config.flakyMaxRetries() > config.maxRetries()) {
                effectiveBudget = quarantineBudget;
            }
            if (attempt >= effectiveBudget) {
                flakyRegistry.recordRun(caseId, true, false, config.flakyThreshold());
                boolean flaky = flakyRegistry.isKnownFlaky(caseId, config.flakyThreshold())
                        || sawRetriable;
                finishCase(caseId, outcome.executedUnit, CaseStatus.FAILED, null,
                        isQuarantine, flaky);
                return;
            }
            attempt++;
        }
    }

    private record AttemptOutcome(
            int executedUnit,
            AttemptStatus status,
            FailureType failureType,
            String message,
            long durationMillis,
            boolean interrupted,
            boolean crashed
    ) {
    }

    private AttemptOutcome runAttempt(int unit, String caseId, TestExecutable executable,
                                      int attemptNumber, boolean isQuarantine) {
        TestCase tc = byId.get(caseId);
        Thread runner = Thread.currentThread();
        long start = System.nanoTime();
        boolean[] deadlineExceeded = {false};
        ScheduledFuture<?> timeoutTask = null;
        ResourceLease lease = null;
        boolean crashSignalled = false;
        try {
            ResourceManager rm = isQuarantine ? quarantineResources : stableResources;
            lease = rm.acquire(tc, this::isAborted);
            if (isAborted()) {
                throw new InterruptedException("run aborted");
            }
            final long timeoutMillis = config.defaultTimeoutMillis();
            if (timeoutMillis > 0) {
                timeoutTask = timer.schedule(() -> {
                    deadlineExceeded[0] = true;
                    runner.interrupt();
                }, timeoutMillis, TimeUnit.MILLISECONDS);
            }

            View view = new View(caseId, tc.historicalDurationMillis());
            Ctx ctx = new Ctx(attemptNumber, ShardPlanner.unitId(unit), cancelRequested);
            executable.execute(view, ctx);
            long dur = elapsed(start);
            if (deadlineExceeded[0]) {
                return new AttemptOutcome(unit, AttemptStatus.TIMEOUT, FailureType.RETRIABLE,
                        "case timed out after " + timeoutMillis + "ms", dur, true, false);
            }
            return new AttemptOutcome(unit, AttemptStatus.PASSED, null, null, dur, false, false);
        } catch (UnitCrashException crash) {
            crashSignalled = true;
            throw crash;
        } catch (InterruptedException ie) {
            long dur = elapsed(start);
            if (deadlineExceeded[0]) {
                return new AttemptOutcome(unit, AttemptStatus.TIMEOUT, FailureType.RETRIABLE,
                        "case timed out (interrupted)", dur, true, false);
            }
            return new AttemptOutcome(unit, AttemptStatus.INTERRUPTED, FailureType.RETRIABLE,
                    ie.getMessage() == null ? "interrupted" : ie.getMessage(), dur, true, false);
        } catch (Throwable t) {
            long dur = elapsed(start);
            FailureType type = CLASSIFIER.classify(t);
            return new AttemptOutcome(unit, AttemptStatus.FAILED, type,
                    String.valueOf(t.getMessage()), dur, false, false);
        } finally {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
            if (lease != null) {
                lease.close();
            }
            if (!crashSignalled) {
                synchronized (lock) {
                    inFlight.remove(caseId);
                    if (caseId.equals(inflightByUnit[unit])) {
                        inflightByUnit[unit] = null;
                    }
                    lock.notifyAll();
                }
            }
        }
    }

    private static long elapsed(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    // ------------------------------------------------------------- crash handling

    /**
     * JVM uncaught handler for a dead worker thread. The in-flight attempt is
     * recorded CRASHED (its history is kept), then the unit's remaining work is
     * redistributed to survivors; with no survivors the cases are SKIPPED.
     */
    private void onUnitDied(int unit, Throwable error) {
        synchronized (lock) {
            if (!deadUnits.add(unit)) {
                return;
            }
            aliveUnits--;

            String inflightCase = inflightByUnit[unit];
            if (inflightCase != null) {
                inFlight.remove(inflightCase);
                inflightByUnit[unit] = null;
                AttemptRecord crashedAttempt = new AttemptRecord(
                        store.attemptsOf(inflightCase).size() + 1,
                        ShardPlanner.unitId(unit), ShardPlanner.unitId(unit),
                        AttemptStatus.CRASHED, FailureType.RETRIABLE,
                        "execution unit crashed: " + safeMessage(error),
                        0L, false, true);
                store.appendAttempt(inflightCase, crashedAttempt);
            }

            List<Integer> survivors = new ArrayList<>();
            for (int i = 0; i < unitQueues.length; i++) {
                if (!deadUnits.contains(i)) {
                    survivors.add(i);
                }
            }

            List<String> work = new ArrayList<>(unitQueues[unit]);
            unitQueues[unit].clear();
            if (inflightCase != null) {
                work.add(0, inflightCase);
            }

            if (survivors.isEmpty()) {
                for (String id : work) {
                    if (!store.isFinalized(id)) {
                        commitSkipped(id,
                                aborted ? (timedOut ? SkipReason.RUN_TIMEOUT : SkipReason.CANCELLED)
                                        : SkipReason.UNIT_CRASHED);
                    }
                }
            } else {
                for (String id : work) {
                    if (store.isFinalized(id)) {
                        continue;
                    }
                    int target = pickRedistributionTarget(survivors);
                    unitOwners[unit].remove(id);
                    unitOwners[target].add(id);
                    unitQueues[target].addLast(id);
                }
            }
            lock.notifyAll();
        }
    }

    private int pickRedistributionTarget(List<Integer> survivors) {
        int best = survivors.get(0);
        int bestSize = unitQueues[best].size();
        for (int i = 1; i < survivors.size(); i++) {
            int u = survivors.get(i);
            int size = unitQueues[u].size();
            if (size < bestSize || (size == bestSize && u < best)) {
                best = u;
                bestSize = size;
            }
        }
        return best;
    }

    // ------------------------------------------------------------- finalization

    private void recordAttempt(String caseId, int assignedUnit, int executedUnit,
                               AttemptStatus status, FailureType type, String message,
                               long durationMillis, boolean interrupted, boolean crashed) {
        AttemptRecord rec = new AttemptRecord(
                store.attemptsOf(caseId).size() + 1,
                ShardPlanner.unitId(assignedUnit),
                ShardPlanner.unitId(executedUnit),
                status, type, message, durationMillis, interrupted, crashed);
        store.appendAttempt(caseId, rec);
    }

    private void finishCase(String caseId, int executedUnit, CaseStatus status,
                            SkipReason reason, boolean isQuarantine, boolean flaky) {
        String planned = plannedUnit.get(caseId);
        String finalUnit = ShardPlanner.unitId(executedUnit);
        store.finalizeCase(caseId, status, reason, planned, finalUnit, flaky, isQuarantine);
        synchronized (lock) {
            inFlight.remove(caseId);
            afterCaseCommitted();
            lock.notifyAll();
        }
    }

    /** Must be called while holding the scheduler lock. */
    private void commitSkipped(String caseId, SkipReason reason) {
        inFlight.remove(caseId);
        store.finalizeCase(caseId, CaseStatus.SKIPPED, reason,
                plannedUnit.get(caseId), null, false, quarantined.contains(caseId));
    }

    private void skipQueueOnAbort(int unit) {
        // must be called under lock
        for (String id : new ArrayList<>(unitQueues[unit])) {
            if (!store.isFinalized(id)) {
                commitSkipped(id, timedOut ? SkipReason.RUN_TIMEOUT : SkipReason.CANCELLED);
            }
        }
        unitQueues[unit].clear();
        afterCaseCommitted();
    }

    /** Opens the quarantine lane once every stable case has a committed result. */
    private void afterCaseCommitted() {
        // must be called under lock
        if (!stablePhaseOpen) {
            return;
        }
        for (TestCase tc : cases) {
            if (quarantined.contains(tc.id())) {
                continue;
            }
            if (!store.isFinalized(tc.id())) {
                return; // stable work outstanding
            }
        }
        for (String id : inFlight) {
            if (!quarantined.contains(id)) {
                return;
            }
        }
        openQuarantineLaneLocked();
    }

    private void openQuarantineLaneLocked() {
        if (!stablePhaseOpen) {
            return;
        }
        stablePhaseOpen = false;
        if (aborted) {
            List<String> rest = new ArrayList<>(quarantinePending);
            quarantinePending.clear();
            SkipReason reason = timedOut ? SkipReason.RUN_TIMEOUT : SkipReason.CANCELLED;
            for (String id : rest) {
                if (!store.isFinalized(id)) {
                    store.finalizeCase(id, CaseStatus.SKIPPED, reason,
                            plannedUnit.get(id), null, true, true);
                }
            }
            return;
        }
        // deterministic round-robin across alive units
        List<String> ids = new ArrayList<>(quarantinePending);
        quarantinePending.clear();
        ids.sort(String::compareTo);
        int cursor = 0;
        for (String id : ids) {
            int target = -1;
            for (int t = 0; t < unitQueues.length; t++) {
                int u = (t + cursor) % unitQueues.length;
                if (!deadUnits.contains(u)) {
                    target = u;
                    break;
                }
            }
            if (target < 0) {
                store.finalizeCase(id, CaseStatus.SKIPPED, SkipReason.UNIT_CRASHED,
                        plannedUnit.get(id), null, true, true);
                continue;
            }
            unitOwners[target].add(id);
            unitQueues[target].addLast(id);
            cursor = target + 1;
        }
    }

    private boolean allDoneLocked() {
        for (Deque<String> q : unitQueues) {
            if (!q.isEmpty()) {
                return false;
            }
        }
        if (!inFlight.isEmpty()) {
            return false;
        }
        if (stablePhaseOpen && !quarantinePending.isEmpty()) {
            // only acceptable if stable work is all done -> lane should have opened
            for (TestCase tc : cases) {
                if (!quarantined.contains(tc.id()) && !store.isFinalized(tc.id())) {
                    return false;
                }
            }
            return false; // lane transition pending
        }
        for (String id : byId.keySet()) {
            if (!store.isFinalized(id)) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------- ctx/view

    private record View(String id, long historicalDurationMillis) implements TestCaseView {
    }

    private record Ctx(int attemptNumber, String unitId, AtomicBoolean cancelled)
            implements ExecutionContext {
        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public void checkCancelled() throws InterruptedException {
            if (cancelled.get()) {
                throw new InterruptedException("cancelled");
            }
        }
    }

    private static String safeMessage(Throwable t) {
        return t == null || t.getMessage() == null ? "unknown crash" : t.getMessage();
    }
}
