package com.github.guoyyit.testorchestrationjava.orchestration.shard;

import java.util.List;

/**
 * One shard: the execution unit id and the ordered (dependency-respecting)
 * list of case ids assigned to it.
 */
public record ShardAssignment(String unitId, List<String> caseIds) {
    public ShardAssignment {
        caseIds = List.copyOf(caseIds);
    }
}
