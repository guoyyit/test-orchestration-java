package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.SkipReason;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.tc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DependencyTest {

    private OrchestrationConfig cfg(int units) {
        return OrchestrationConfig.builder()
                .unitCount(units)
                .maxConcurrency(units)
                .seed(1L)
                .build();
    }

    @Test
    void failedDependencyMarksDownstreamAsSkippedNotFailed() {
        TestCase a = tc("a", 1);
        TestCase b = tc("b", 1, "a");
        TestCase c = tc("c", 1, "b");

        TestOrchestrator orch = new TestOrchestrator(cfg(3));
        RunResult result = orch.run(List.of(a, b, c),
                TestSupport.executor().fail("a").pass("b").pass("c").build());

        assertEquals(Set.of(), result.passedIds());
        assertEquals(Set.of("a"), result.failedIds());
        assertEquals(Set.of("b", "c"), result.skippedIds());
        assertEquals(SkipReason.DEPENDENCY_FAILED, result.resultOf("b").skipReason());
        assertEquals(SkipReason.DEPENDENCY_FAILED, result.resultOf("c").skipReason());
        // skipped cases never attempted
        assertTrue(result.resultOf("b").attempts().isEmpty());
        assertEquals(CaseStatus.SKIPPED, result.resultOf("c").status());
    }

    @Test
    void passingChainExecutesInOrderAndAllPass() {
        TestCase a = tc("a", 1);
        TestCase b = tc("b", 1, "a");
        TestCase c = tc("c", 1, "b");

        TestOrchestrator orch = new TestOrchestrator(cfg(2));
        RunResult result = orch.run(List.of(a, b, c),
                TestSupport.executor().pass("a").pass("b").pass("c").build());

        assertEquals(Set.of("a", "b", "c"), result.passedIds());
        assertEquals(Set.of(), result.failedIds());
        assertEquals(Set.of(), result.skippedIds());
    }

    @Test
    void skippedDependencyAlsoSkipsTransitiveDependents() {
        TestCase a = tc("a", 1);
        TestCase b = tc("b", 1, "a"); // skipped because a fails
        TestCase d = tc("d", 1);
        TestCase c = tc("c", 1, "b", "d"); // depends on both: must still skip

        TestOrchestrator orch = new TestOrchestrator(cfg(4));
        RunResult result = orch.run(List.of(a, b, c, d),
                TestSupport.executor().fail("a").pass("b").pass("c").pass("d").build());

        assertEquals(Set.of("d"), result.passedIds());
        assertEquals(Set.of("a"), result.failedIds());
        assertEquals(Set.of("b", "c"), result.skippedIds());
        assertFalse(result.resultOf("c").attempts().isEmpty() == false
                && result.resultOf("c").status() == CaseStatus.PASSED);
    }

    @Test
    void disjointChainsRunAcrossUnits() {
        TestCase a1 = tc("a1", 1);
        TestCase a2 = tc("a2", 1, "a1");
        TestCase b1 = tc("b1", 1);
        TestCase b2 = tc("b2", 1, "b1");

        TestOrchestrator orch = new TestOrchestrator(cfg(2));
        RunResult result = orch.run(List.of(a1, a2, b1, b2),
                TestSupport.executor().pass("a1").pass("a2").pass("b1").pass("b2").build());
        assertEquals(Set.of("a1", "a2", "b1", "b2"), result.passedIds());
    }
}
