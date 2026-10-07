package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.exec.OrchestrationConfig;
import com.github.guoyyit.testorchestrationjava.orchestration.exec.Orchestrator;
import com.github.guoyyit.testorchestrationjava.orchestration.model.ResourceQuota;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunSummary;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCaseSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrchestratorQuotaTest {

    private static TestCaseSpec dbCase(String id) {
        return new TestCaseSpec(id, Set.of(), Set.of(), Set.of("db"), Duration.ZERO, false);
    }

    @Test
    void quotaIsNeverExceededAndExcessCasesQueue() {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                4, List.of(new ResourceQuota("db", 2)), null, Duration.ofSeconds(10), 0L));
        RunSummary summary = o.run(List.of(
                dbCase("q1"), dbCase("q2"), dbCase("q3"), dbCase("q4"), dbCase("q5")),
                id -> {
                    int now = active.incrementAndGet();
                    maxObserved.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(40);
                    } finally {
                        active.decrementAndGet();
                    }
                });
        assertEquals(5, summary.passed().size(), "queued cases must eventually run");
        assertTrue(maxObserved.get() <= 2,
                "db quota of 2 must never be oversubscribed, saw " + maxObserved.get());
    }

    @Test
    void untaggedCasesAreNotBlockedByQuota() {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                3, List.of(new ResourceQuota("db", 1)), null, Duration.ofSeconds(10), 0L));
        RunSummary summary = o.run(List.of(
                dbCase("tagged"),
                new TestCaseSpec("free-1", Set.of(), Set.of(), Set.of(), Duration.ZERO, false),
                new TestCaseSpec("free-2", Set.of(), Set.of(), Set.of(), Duration.ZERO, false)),
                id -> {
                    int now = active.incrementAndGet();
                    maxObserved.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(40);
                    } finally {
                        active.decrementAndGet();
                    }
                });
        assertEquals(3, summary.passed().size());
        assertTrue(maxObserved.get() >= 2, "untagged cases should run alongside the quota-limited one");
    }
}
