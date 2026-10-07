package com.github.guoyyit.testorchestrationjava.orchestration.shard;

import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCaseSpec;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic load-balancing shard planner.
 *
 * <p>Algorithm: LPT (longest-processing-time) greedy. Cases are ordered by
 * historical duration descending; ties are broken by a seeded hash of the case
 * id, so the ordering — and therefore the whole plan — is a pure function of
 * (input, seed). Each case is placed on the shard with the lowest current
 * cost, where cost = accumulated millis + {@value #TAG_PENALTY_MILLIS} per
 * already-placed case sharing a resource tag. This spreads both runtime and
 * same-tag cases across shards. Same input + same seed always yields the same
 * plan.</p>
 */
public class ShardPlanner {

    /** Virtual penalty applied per same-tag case already on a shard. */
    static final long TAG_PENALTY_MILLIS = 1;

    private final long seed;

    public ShardPlanner(long seed) {
        this.seed = seed;
    }

    public ShardPlan plan(List<TestCaseSpec> cases, int shardCount) {
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1");
        }
        List<TestCaseSpec> ordered = new ArrayList<>(cases);
        ordered.sort(Comparator
                .comparingLong((TestCaseSpec c) -> c.historicalDuration().toMillis()).reversed()
                .thenComparingLong(c -> seededHash(c.id()))
                .thenComparing(TestCaseSpec::id));

        long[] loads = new long[shardCount];
        List<List<String>> assignments = new ArrayList<>();
        Map<String, int[]> tagCounts = new HashMap<>();
        for (int i = 0; i < shardCount; i++) {
            assignments.add(new ArrayList<>());
        }

        for (TestCaseSpec c : ordered) {
            int target = cheapestShard(c, loads, tagCounts, shardCount);
            assignments.get(target).add(c.id());
            loads[target] += c.historicalDuration().toMillis();
            for (String tag : c.resourceTags()) {
                tagCounts.computeIfAbsent(tag, k -> new int[shardCount])[target]++;
            }
        }

        List<ShardPlan.Shard> shards = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            shards.add(new ShardPlan.Shard(i, assignments.get(i), loads[i]));
        }
        return new ShardPlan(shards);
    }

    private int cheapestShard(TestCaseSpec c, long[] loads, Map<String, int[]> tagCounts, int shardCount) {
        int best = 0;
        long bestCost = Long.MAX_VALUE;
        for (int i = 0; i < shardCount; i++) {
            long cost = loads[i];
            for (String tag : c.resourceTags()) {
                int[] counts = tagCounts.get(tag);
                if (counts != null) {
                    cost += TAG_PENALTY_MILLIS * counts[i];
                }
            }
            if (cost < bestCost) {
                bestCost = cost;
                best = i;
            }
        }
        return best;
    }

    /** Stable 64-bit hash of (seed, id); independent of JVM hash randomization. */
    private long seededHash(String id) {
        long h = seed;
        for (int i = 0; i < id.length(); i++) {
            h = 31 * h + id.charAt(i);
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        return h;
    }
}
