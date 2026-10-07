package com.github.guoyyit.testorchestrationjava.orchestration.model;

import java.util.Map;
import java.util.Set;

/**
 * Deterministic aggregated outcome of one run: the passed/failed/skipped sets
 * are disjoint and their union equals the submitted case set.
 */
public record RunSummary(
        String runId,
        Set<String> passed,
        Set<String> failed,
        Set<String> skipped,
        Map<String, TestResult> results) {

    public RunSummary {
        passed = passed == null ? Set.of() : Set.copyOf(passed);
        failed = failed == null ? Set.of() : Set.copyOf(failed);
        skipped = skipped == null ? Set.of() : Set.copyOf(skipped);
        results = results == null ? Map.of() : Map.copyOf(results);
    }
}
