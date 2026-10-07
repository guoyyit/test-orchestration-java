package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.SkipReason;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.label;
import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.mutex;
import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.tc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeCasesTest {

    @Test
    void dependencyFailureCombinedWithMutexStillClassifiesSkipped() {
        // m0 and m1 are mutually exclusive; m1 also depends on failing "pre".
        TestCase pre = tc("pre", 1);
        TestCase m0 = mutex("m0", 1, "g");
        TestCase m1 = mutex("m1", 1, "g", "pre");

        RunResult result = new TestOrchestrator(
                OrchestrationConfig.builder().unitCount(3).maxConcurrency(3).seed(0L).build())
                .run(List.of(pre, m0, m1),
                        TestSupport.executor().fail("pre").pass("m0").pass("m1").build());

        assertEquals(Set.of("m0"), result.passedIds());
        assertEquals(Set.of("pre"), result.failedIds());
        assertEquals(Set.of("m1"), result.skippedIds());
        assertEquals(SkipReason.DEPENDENCY_FAILED, result.resultOf("m1").skipReason());
        CaseResult m1Result = result.resultOf("m1");
        assertTrue(m1Result.attempts().isEmpty());
    }

    @Test
    void crashOfSoleUnitSkipsUnfinishedWithCrashedAttemptRecorded() {
        TestCase a = tc("a", 1);
        TestCase b = tc("b", 1);
        RunResult result = new TestOrchestrator(
                OrchestrationConfig.builder().unitCount(1).maxConcurrency(1)
                        .maxRetries(0).seed(0L).build())
                .run(List.of(a, b),
                        TestSupport.executor().alwaysCrash("a").pass("b").build());
        // single unit dies with no survivor: both end skipped, crash is traceable
        assertEquals(Set.of(), result.passedIds());
        assertEquals(Set.of("a", "b"), result.skippedIds());
        CaseResult ar = result.resultOf("a");
        assertEquals(SkipReason.UNIT_CRASHED, ar.skipReason());
        assertTrue(ar.attempts().stream()
                .anyMatch(x -> x.status() == com.github.guoyyit.testorchestrationjava.orchestration
                        .model.AttemptStatus.CRASHED));
    }

    @Test
    void overQuotaCasesQueueAndEventuallyRun() throws Exception {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        TestCase dbCase = label("d", 1, "db");

        var builder = TestSupport.executor();
        builder.body("d0", (v, c) -> guarded(active, peak, release));
        builder.body("d1", (v, c) -> guarded(active, peak, release));
        builder.body("d2", (v, c) -> guarded(active, peak, release));

        List<TestCase> cases = List.of(
                label("d0", 1, "db"), label("d1", 1, "db"), label("d2", 1, "db"));
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(150);
                release.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        releaser.start();
        RunResult result = new TestOrchestrator(
                OrchestrationConfig.builder().unitCount(3).maxConcurrency(3)
                        .resourceQuotas(Map.of("db", 1))
                        .seed(0L).build())
                .run(cases, builder.build());
        releaser.join();
        assertEquals(Set.of("d0", "d1", "d2"), result.passedIds());
        assertEquals(1, peak.get(), "db quota must queue excess cases, peak=" + peak.get());
    }

    private static void guarded(AtomicInteger active, AtomicInteger peak, CountDownLatch gate) {
        int now = active.incrementAndGet();
        peak.updateAndGet(p -> Math.max(p, now));
        try {
            gate.await(5, TimeUnit.SECONDS);
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            active.decrementAndGet();
        }
    }
}
