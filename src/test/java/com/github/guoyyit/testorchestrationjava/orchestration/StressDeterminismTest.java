package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.flaky.FlakyRegistry;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.tc;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StressDeterminismTest {

    /**
     * Repeated runs with a fresh memory registry each time exercise the
     * concurrent scheduler; aggregates must stay identical and no deadlock may
     * occur (test-level timeout guards that).
     */
    @Test
    @Timeout(60)
    void repeatedConcurrentRunsYieldIdenticalAggregates() {
        List<TestCase> cases = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            String dep = i >= 6 ? "c" + (i - 6) : null;
            cases.add(dep == null ? tc("c" + i, 1 + (i % 3)) : tc("c" + i, 1 + (i % 3), dep));
        }

        RunResult first = null;
        for (int run = 0; run < 20; run++) {
            OrchestrationConfig config = OrchestrationConfig.builder()
                    .unitCount(4).maxConcurrency(4).maxRetries(1)
                    .flakyMaxRetries(2).flakyThreshold(2)
                    .seed(777L).build();
            FlakyRegistry registry = new FlakyRegistry();
            RunResult result = new TestOrchestrator(config, registry).run(cases,
                    TestSupport.executor()
                            .fail("c2")
                            .failRetriable("c9")
                            .build());
            if (first == null) {
                first = result;
            } else {
                assertEquals(first.passedIds(), result.passedIds(), "run " + run);
                assertEquals(first.failedIds(), result.failedIds(), "run " + run);
                assertEquals(first.skippedIds(), result.skippedIds(), "run " + run);
            }
        }
    }
}
