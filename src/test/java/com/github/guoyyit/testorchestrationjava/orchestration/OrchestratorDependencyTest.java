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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrchestratorDependencyTest {

    private static TestCaseSpec spec(String id, Set<String> deps, Set<String> mutex) {
        return new TestCaseSpec(id, deps, mutex, Set.of(), Duration.ZERO, false);
    }

    @Test
    void dependenciesRunBeforeDependents() {
        CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                4, List.of(), null, Duration.ofSeconds(10), 0L));
        RunSummary summary = o.run(List.of(
                spec("child", Set.of("parent"), Set.of()),
                spec("parent", Set.of(), Set.of())),
                id -> order.add(id));
        assertEquals(Set.of("parent", "child"), summary.passed());
        assertTrue(order.indexOf("parent") < order.indexOf("child"),
                "parent must execute before child, got " + order);
    }

    @Test
    void failedDependencySkipsWholeSubtree() {
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                2, List.of(), null, Duration.ofSeconds(10), 0L));
        RunSummary summary = o.run(List.of(
                spec("root", Set.of(), Set.of()),
                spec("mid", Set.of("root"), Set.of()),
                spec("leaf", Set.of("mid"), Set.of()),
                spec("independent", Set.of(), Set.of())),
                id -> {
                    if (id.equals("root")) throw new AssertionError("boom");
                });
        assertEquals(Set.of("independent"), summary.passed());
        assertEquals(Set.of("root"), summary.failed());
        assertEquals(Set.of("mid", "leaf"), summary.skipped(),
                "dependents of a failed case must be SKIPPED, not FAILED");
        // skipped cases carry a traceable reason and zero attempts
        assertEquals(TestStatus.SKIPPED, summary.results().get("mid").status());
        assertEquals(0, summary.results().get("mid").attempts());
    }

    @Test
    void dependencyCycleIsRejected() {
        Orchestrator o = new Orchestrator(OrchestrationConfig.defaults());
        assertThrows(IllegalArgumentException.class, () -> o.run(List.of(
                spec("a", Set.of("b"), Set.of()),
                spec("b", Set.of("a"), Set.of())), id -> {
        }));
    }

    @Test
    void unknownDependencyIsRejected() {
        Orchestrator o = new Orchestrator(OrchestrationConfig.defaults());
        assertThrows(IllegalArgumentException.class, () -> o.run(List.of(
                spec("a", Set.of("ghost"), Set.of())), id -> {
        }));
    }

    @Test
    void mutexGroupsNeverRunConcurrently() throws Exception {
        AtomicInteger inSection = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                4, List.of(), null, Duration.ofSeconds(10), 0L));
        RunSummary summary = o.run(List.of(
                spec("m1", Set.of(), Set.of("exclusive")),
                spec("m2", Set.of(), Set.of("exclusive")),
                spec("m3", Set.of(), Set.of("exclusive"))),
                id -> {
                    int now = inSection.incrementAndGet();
                    maxObserved.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(50);
                    } finally {
                        inSection.decrementAndGet();
                    }
                });
        assertEquals(3, summary.passed().size());
        assertEquals(1, maxObserved.get(), "mutex group cases must not overlap");
    }

    @Test
    void disjointCasesRunInParallelWithinConcurrencyLimit() {
        Set<String> concurrent = ConcurrentHashMap.newKeySet();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        Orchestrator o = new Orchestrator(new OrchestrationConfig(
                2, List.of(), null, Duration.ofSeconds(10), 0L));
        o.run(List.of(spec("p1", Set.of(), Set.of()), spec("p2", Set.of(), Set.of()),
                spec("p3", Set.of(), Set.of()), spec("p4", Set.of(), Set.of())),
                id -> {
                    int now = active.incrementAndGet();
                    maxObserved.accumulateAndGet(now, Math::max);
                    concurrent.add(id);
                    try {
                        Thread.sleep(50);
                    } finally {
                        active.decrementAndGet();
                    }
                });
        assertEquals(4, concurrent.size());
        assertTrue(maxObserved.get() <= 2, "concurrency cap must hold, saw " + maxObserved.get());
        assertTrue(maxObserved.get() >= 2, "independent cases should actually overlap");
    }
}
