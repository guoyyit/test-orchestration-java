package com.github.guoyyit.testorchestrationjava.orchestration.model;

import java.util.List;

/**
 * Immutable final conclusion for one case. Exactly one final status per case per run.
 *
 * @param caseId case identifier
 * @param status unique final status
 * @param skipReason present iff status == SKIPPED
 * @param attempts every attempt made, in order; empty if the case never started
 * @param plannedUnitId unit the shard plan assigned the case to
 * @param finalExecutedUnitId unit that executed the decisive attempt, or null if never started
 * @param flaky true if the case was treated as unstable during this run
 * @param quarantined true if the case ran in (or was held out by) the quarantine lane
 */
public record CaseResult(
        String caseId,
        CaseStatus status,
        SkipReason skipReason,
        List<AttemptRecord> attempts,
        String plannedUnitId,
        String finalExecutedUnitId,
        boolean flaky,
        boolean quarantined
) {
    /** Total attempts actually executed (excludes skipped placeholders). */
    public int attemptCount() {
        return attempts == null ? 0 : attempts.size();
    }
}
