package com.github.guoyyit.testorchestrationjava.orchestration.audit;

import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseResult;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlan;
import java.util.List;

/**
 * Complete, serializable audit record of one run: inputs (config digest),
 * the shard plan, every case result with attempts, and the aggregate sets.
 */
public record AuditReport(
        String runId,
        long seed,
        int unitCount,
        boolean cancelled,
        boolean timedOut,
        ShardPlan shardPlan,
        List<CaseResult> caseResults
) {
}
