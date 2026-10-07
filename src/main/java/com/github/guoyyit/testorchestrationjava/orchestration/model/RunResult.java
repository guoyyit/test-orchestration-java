package com.github.guoyyit.testorchestrationjava.orchestration.model;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Immutable result of a whole orchestrated run. The pass/fail/skip sets are
 * disjoint and their union equals the full input case set; results are always
 * ordered by case id so that the same run yields the same aggregates.
 */
public record RunResult(
        String runId,
        List<CaseResult> caseResults,
        boolean cancelled,
        boolean timedOut
) {
    public RunResult {
        caseResults = caseResults.stream()
                .sorted(java.util.Comparator.comparing(CaseResult::caseId))
                .toList();
    }

    public Set<String> passedIds() {
        return idsWith(CaseStatus.PASSED);
    }

    public Set<String> failedIds() {
        return idsWith(CaseStatus.FAILED);
    }

    public Set<String> skippedIds() {
        return idsWith(CaseStatus.SKIPPED);
    }

    private Set<String> idsWith(CaseStatus status) {
        return caseResults.stream()
                .filter(r -> r.status() == status)
                .map(CaseResult::caseId)
                .collect(Collectors.toUnmodifiableSet());
    }

    public CaseResult resultOf(String caseId) {
        return caseResults.stream()
                .filter(r -> r.caseId().equals(caseId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown case: " + caseId));
    }
}
