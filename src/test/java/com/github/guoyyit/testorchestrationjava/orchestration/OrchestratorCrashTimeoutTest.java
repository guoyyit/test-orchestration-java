package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.exec.OrchestrationConfig;
import com.github.guoyyit.testorchestrationjava.orchestration.exec.Orchestrator;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunSummary;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCaseSpec;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrchestratorCrashTimeoutTest {

    private static TestCaseSpec spec(String id) {
        return new TestCaseSpec(id, Set.of(), Set.of(), Set.of(), Duration.ZERO, false);
    }

    @Test
    void timeoutIsRecordedAsFailureNotPass() {
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                1, List.of(), null, Duration.ofMillis(200), 0L));
        RunSummary summary = o.run(List.of(spec("hangs"), spec("normal")), id -> {
            if (id.equals("hangs")) {
                Thread.sleep(30_000); // will be interrupted by timeout
            }
        });
        assertEquals(Set.of("hangs"), summary.failed());
        assertEquals(Set.of("normal"), summary.passed());
        assertTrue(summary.results().get("hangs").error().contains("timeout"),
                "timeout must be visible in the result");
    }

    @Test
    void crashingCaseFailsWithoutKillingTheRun() {
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                2, List.of(), null, Duration.ofSeconds(5), 0L));
        RunSummary summary = o.run(List.of(spec("crashes"), spec("survivor")), id -> {
            if (id.equals("crashes")) {
                throw new OutOfMemoryError("simulated executor crash");
            }
        });
        assertEquals(Set.of("crashes"), summary.failed());
        assertEquals(Set.of("survivor"), summary.passed(),
                "a crash in one case must not lose other results");
    }

    @Test
    void cancelKeepsCommittedResultsAndSkipsUnfinished() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                1, List.of(), null, Duration.ofSeconds(30), 0L));
        AtomicReference<RunSummary> ref = new AtomicReference<>();
        Thread runner = new Thread(() -> ref.set(o.run(
                List.of(spec("first"), spec("second"), spec("third")), id -> {
                    if (id.equals("first")) {
                        firstStarted.countDown();
                        Thread.sleep(60_000); // long-running, will be cancelled
                    }
                })));
        runner.start();
        assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
        // let "first" get interrupted mid-attempt
        Thread.sleep(200);
        o.cancel();
        runner.join(10_000);
        assertFalse(runner.isAlive(), "run must terminate after cancel");
        RunSummary summary = ref.get();
        assertTrue(summary.passed().isEmpty(), "interrupted case must not count as passed");
        assertEquals(Set.of("first", "second", "third"),
                union(summary.skipped(), union(summary.failed(), summary.passed())),
                "every case must have exactly one accounted outcome");
        assertTrue(summary.skipped().containsAll(List.of("second", "third")),
                "never-started cases must be skipped");
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        Set<String> s = new java.util.HashSet<>(a);
        s.addAll(b);
        return s;
    }
}
