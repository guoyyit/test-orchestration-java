package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.flaky.FlakyRegistry;
import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.IsolationStrategy;
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

class FlakyRetryTest {

    private OrchestrationConfig cfg(int retries) {
        return OrchestrationConfig.builder()
                .unitCount(2)
                .maxConcurrency(2)
                .maxRetries(retries)
                .flakyThreshold(1)
                .flakyMaxRetries(3)
                .seed(3L)
                .build();
    }

    @Test
    void deterministicFailureIsNeverRetried() {
        TestCase a = tc("a", 1);
        TestOrchestrator orch = new TestOrchestrator(cfg(3));
        RunResult result = orch.run(List.of(a), TestSupport.executor().fail("a").build());
        CaseResult cr = result.resultOf("a");
        assertEquals(1, cr.attempts().size(), "assertion failure must not be retried");
        assertEquals(AttemptStatus.FAILED, cr.attempts().get(0).status());
        assertEquals(Set.of("a"), result.failedIds());
    }

    @Test
    void retriableFailureIsRetriedAndPassesWithinBudget() {
        TestCase a = tc("a", 1);
        TestOrchestrator orch = new TestOrchestrator(cfg(2));
        RunResult result = orch.run(List.of(a),
                TestSupport.executor().flaky("a", 2).build());
        CaseResult cr = result.resultOf("a");
        assertEquals(3, cr.attempts().size());
        assertEquals(AttemptStatus.FAILED, cr.attempts().get(0).status());
        assertEquals(AttemptStatus.FAILED, cr.attempts().get(1).status());
        assertEquals(AttemptStatus.PASSED, cr.attempts().get(2).status());
        assertEquals(Set.of("a"), result.passedIds());
    }

    @Test
    void exhaustedRetriesHaveUniqueFailedConclusion() {
        TestCase a = tc("a", 1);
        TestOrchestrator orch = new TestOrchestrator(cfg(1));
        RunResult result = orch.run(List.of(a),
                TestSupport.executor().failRetriable("a").build());
        CaseResult cr = result.resultOf("a");
        assertEquals(2, cr.attempts().size());
        assertEquals(Set.of("a"), result.failedIds());
        assertEquals(Set.of(), result.passedIds());
        assertTrue(cr.flaky());
    }

    @Test
    void knownFlakyCaseRunsInQuarantineLaneAfterStableCases() {
        TestCase stable = tc("stable", 1);
        TestCase flaky = tc("flaky", 1);

        FlakyRegistry registry = new FlakyRegistry();
        registry.recordRun("flaky", true, false, 1);

        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(2)
                .maxConcurrency(2)
                .isolationStrategy(IsolationStrategy.QUARANTINE_AFTER)
                .flakyThreshold(1)
                .flakyMaxRetries(2)
                .quarantineConcurrency(1)
                .seed(5L)
                .build();
        TestOrchestrator orch = new TestOrchestrator(config, registry);
        RunResult result = orch.run(List.of(stable, flaky),
                TestSupport.executor().pass("stable").flaky("flaky", 1).build());

        assertEquals(Set.of("stable", "flaky"), result.passedIds());
        CaseResult fr = result.resultOf("flaky");
        assertTrue(fr.quarantined(), "flaky case must be marked quarantined");
        assertTrue(fr.flaky());
    }

    @Test
    void skipQuarantinedStrategyHoldsOutKnownFlakyCase() {
        TestCase stable = tc("stable", 1);
        TestCase flaky = tc("flaky", 1);

        FlakyRegistry registry = new FlakyRegistry();
        registry.recordRun("flaky", true, false, 1);

        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(2)
                .maxConcurrency(2)
                .isolationStrategy(IsolationStrategy.SKIP_QUARANTINED)
                .flakyThreshold(1)
                .build();
        TestOrchestrator orch = new TestOrchestrator(config, registry);
        RunResult result = orch.run(List.of(stable, flaky),
                TestSupport.executor().pass("stable").pass("flaky").build());

        assertEquals(Set.of("stable"), result.passedIds());
        assertEquals(Set.of("flaky"), result.skippedIds());
        assertEquals(SkipReason.QUARANTINED, result.resultOf("flaky").skipReason());
        assertTrue(result.resultOf("flaky").attempts().isEmpty());
    }

    @Test
    void retriesDoNotMaskDeterministicFailureAfterRetriableSuccess() {
        // First attempt retriable-fails then body flips to deterministic failure:
        // deterministic failure ends the case immediately as FAILED.
        TestCase a = tc("a", 1);
        TestOrchestrator orch = new TestOrchestrator(cfg(3));
        RunResult result = orch.run(List.of(a),
                TestSupport.executor()
                        .body("a", (v, c) -> {
                            if (c.attemptNumber() == 1) {
                                throw new com.github.guoyyit.testorchestrationjava.orchestration
                                        .executor.TestFailureException(
                                        com.github.guoyyit.testorchestrationjava.orchestration
                                                .model.FailureType.RETRIABLE, "blip");
                            }
                            throw new AssertionError("hard failure");
                        })
                        .build());
        CaseResult cr = result.resultOf("a");
        assertEquals(2, cr.attempts().size());
        assertEquals(AttemptStatus.FAILED, cr.attempts().get(1).status());
        assertFalse(cr.status() == com.github.guoyyit.testorchestrationjava.orchestration
                .model.CaseStatus.PASSED);
    }
}
