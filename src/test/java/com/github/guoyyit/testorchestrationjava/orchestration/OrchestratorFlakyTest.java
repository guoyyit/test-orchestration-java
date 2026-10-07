package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.exec.OrchestrationConfig;
import com.github.guoyyit.testorchestrationjava.orchestration.exec.Orchestrator;
import com.github.guoyyit.testorchestrationjava.orchestration.exec.RetryPolicy;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunSummary;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCaseSpec;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrchestratorFlakyTest {

    private static TestCaseSpec spec(String id, boolean flaky) {
        return new TestCaseSpec(id, Set.of(), Set.of(), Set.of(), Duration.ZERO, flaky);
    }

    private static OrchestrationConfig config(int maxAttempts) {
        return new OrchestrationConfig(2, List.of(),
                new RetryPolicy(maxAttempts, true), Duration.ofSeconds(10), 0L);
    }

    @Test
    void flakyCasePassesAfterRetryWithFullAuditTrail() {
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        Orchestrator o = new Orchestrator(config(3));
        RunSummary summary = o.run(List.of(spec("flaky-1", true)), id -> {
            if (calls.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet() < 3) {
                throw new AssertionError("unstable environment");
            }
        });
        assertEquals(Set.of("flaky-1"), summary.passed());
        TestResult r = summary.results().get("flaky-1");
        assertEquals(3, r.attempts(), "every attempt must be recorded");
        assertEquals(3, r.history().size());
        assertEquals(TestStatus.FAILED, r.history().get(0).status());
        assertEquals(TestStatus.PASSED, r.history().get(2).status());
        assertNotNull(r.executorId(), "attempts must be traceable to an executor unit");
        assertEquals(1, calls.get("flaky-1").get() == 3 ? 1 : 0);
    }

    @Test
    void deterministicCaseIsNeverRetried() {
        AtomicInteger calls = new AtomicInteger();
        Orchestrator o = new Orchestrator(config(5));
        RunSummary summary = o.run(List.of(spec("stable", false)), id -> {
            calls.incrementAndGet();
            throw new AssertionError("deterministic failure");
        });
        assertEquals(Set.of("stable"), summary.failed());
        assertEquals(1, calls.get(), "retry must not mask a deterministic failure");
        assertEquals(1, summary.results().get("stable").attempts());
    }

    @Test
    void flakyCaseFailsWhenRetriesExhausted() {
        AtomicInteger calls = new AtomicInteger();
        Orchestrator o = new Orchestrator(config(2));
        RunSummary summary = o.run(List.of(spec("always-broken", true)), id -> {
            calls.incrementAndGet();
            throw new AssertionError("still broken");
        });
        assertEquals(Set.of("always-broken"), summary.failed());
        assertEquals(2, calls.get(), "flaky case gets exactly maxAttempts tries");
        TestResult r = summary.results().get("always-broken");
        assertEquals(TestStatus.FAILED, r.status());
        assertTrue(r.error().contains("still broken"), "final error must be preserved");
    }

    @Test
    void finalConclusionIsUniquePerCase() {
        Orchestrator o = new Orchestrator(config(3));
        RunSummary summary = o.run(List.of(spec("f", true), spec("s", false)), id -> {
            if (id.equals("f")) throw new AssertionError("x");
        });
        Set<String> all = new java.util.HashSet<>();
        all.addAll(summary.passed());
        all.addAll(summary.failed());
        all.addAll(summary.skipped());
        assertEquals(Set.of("f", "s"), all, "union of outcome sets covers every case");
        assertEquals(2, summary.results().size(), "exactly one final result per case");
    }
}
