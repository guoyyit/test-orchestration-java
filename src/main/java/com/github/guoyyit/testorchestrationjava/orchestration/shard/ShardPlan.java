package com.github.guoyyit.testorchestrationjava.orchestration.shard;

import java.util.List;

/**
 * Output of the shard planner: an ordered list of shards, each carrying the
 * case ids assigned to it and the estimated load in milliseconds.
 */
public record ShardPlan(List<Shard> shards) {

    public ShardPlan {
        shards = shards == null ? List.of() : List.copyOf(shards);
    }

    public record Shard(int index, List<String> caseIds, long estimatedMillis) {
        public Shard {
            caseIds = caseIds == null ? List.of() : List.copyOf(caseIds);
        }
    }
}
