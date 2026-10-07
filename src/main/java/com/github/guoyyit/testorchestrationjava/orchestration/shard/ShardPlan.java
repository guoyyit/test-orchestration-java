package com.github.guoyyit.testorchestrationjava.orchestration.shard;

import java.util.List;

/**
 * Deterministic mapping of every input case to exactly one execution unit.
 * Pure function of (ordered case definitions, unit count, seed).
 */
public record ShardPlan(long seed, List<ShardAssignment> shards) {
    public ShardPlan {
        shards = List.copyOf(shards);
    }
}
