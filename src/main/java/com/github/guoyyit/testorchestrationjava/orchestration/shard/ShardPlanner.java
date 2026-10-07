package com.github.guoyyit.testorchestrationjava.orchestration.shard;

import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Assigns cases to execution units.
 *
 * <p>Semantics: dependency-aware, resource-label weighted load balancing with a
 * seeded deterministic tie-break. Cases are visited in a topological order
 * (a case is only assigned once all its dependencies were assigned) and each
 * case is greedily placed on the unit minimizing the resulting shard load.
 * The load score is the largest of: total historical duration and the duration
 * attributed to any single resource label, so a label-heavy set of cases is
 * spread across units instead of piling onto one shard. Equal scores are broken
 * by a stable seeded hash and finally by unit id, so the same ordered input,
 * unit count and seed ALWAYS produces the same plan.</p>
 *
 * <p>Dependencies and mutexes are also honored globally at execution time, so
 * placing dependent/mutually-exclusive cases on different units is safe; the
 * planner nevertheless prefers keeping dependency chains together when it does
 * not change the load decision.</p>
 */
public final class ShardPlanner {

    private ShardPlanner() {
    }

    /**
     * Builds a shard plan.
     *
     * @throws IllegalArgumentException on duplicate/unknown dependencies, unknown ids or dependency cycles
     */
    public static ShardPlan plan(List<TestCase> cases, int unitCount, long seed) {
        if (unitCount <= 0) {
            throw new IllegalArgumentException("unitCount must be positive, got " + unitCount);
        }
        List<TestCase> ordered = topologicalOrder(cases);

        // unit id -> total historical load
        long[] totalLoad = new long[unitCount];
        // unit id -> label -> attributed historical load
        @SuppressWarnings("unchecked")
        Map<String, Long>[] labelLoad = new Map[unitCount];
        List<List<String>> assigned = new ArrayList<>(unitCount);
        long[] tieSeed = new long[unitCount];
        for (int i = 0; i < unitCount; i++) {
            labelLoad[i] = new HashMap<>();
            assigned.add(new ArrayList<>());
            tieSeed[i] = mix(seed ^ (i * 0x9E3779B97F4A7C15L));
        }

        for (TestCase tc : ordered) {
            long weight = Math.max(0L, tc.historicalDurationMillis());
            int best = -1;
            long bestScore = Long.MAX_VALUE;
            long bestTie = Long.MAX_VALUE;
            for (int u = 0; u < unitCount; u++) {
                long newTotal = totalLoad[u] + weight;
                long newLabelPeak = 0L;
                for (String label : tc.resourceLabels()) {
                    long cur = labelLoad[u].getOrDefault(label, 0L);
                    newLabelPeak = Math.max(newLabelPeak, cur + weight);
                }
                // also consider labels already present (peak over all labels)
                for (long v : labelLoad[u].values()) {
                    newLabelPeak = Math.max(newLabelPeak, v);
                }
                long score = Math.max(newTotal, newLabelPeak);
                long tie = tieSeed[u] ^ mix(tc.id().hashCode());
                if (score < bestScore || (score == bestScore && tie < bestTie)
                        || (score == bestScore && tie == bestTie && u < best)) {
                    bestScore = score;
                    bestTie = tie;
                    best = u;
                }
            }
            totalLoad[best] += weight;
            for (String label : tc.resourceLabels()) {
                labelLoad[best].merge(label, weight, Long::sum);
            }
            assigned.get(best).add(tc.id());
        }

        List<ShardAssignment> shards = new ArrayList<>(unitCount);
        for (int u = 0; u < unitCount; u++) {
            // order within a shard follows the topological order: dependencies precede dependents
            shards.add(new ShardAssignment(unitId(u), List.copyOf(assigned.get(u))));
        }
        return new ShardPlan(seed, List.copyOf(shards));
    }

    /** Stable execution-unit identifier. */
    public static String unitId(int zeroBasedIndex) {
        return "unit-" + zeroBasedIndex;
    }

    /**
     * Kahn topological sort over the declared dependency graph. Input order is
     * preserved among ready nodes (sorted by id for determinism independent of
     * the input list ordering).
     */
    public static List<TestCase> topologicalOrder(List<TestCase> cases) {
        Map<String, TestCase> byId = new LinkedHashMap<>();
        for (TestCase tc : cases) {
            if (byId.put(tc.id(), tc) != null) {
                throw new IllegalArgumentException("duplicate test case id: " + tc.id());
            }
        }
        Map<String, Integer> remaining = new HashMap<>();
        Map<String, Set<String>> dependents = new HashMap<>();
        for (TestCase tc : cases) {
            Set<String> deps = new HashSet<>(tc.dependsOn());
            if (deps.size() != tc.dependsOn().size()) {
                throw new IllegalArgumentException("duplicate dependency on case: " + tc.id());
            }
            for (String dep : deps) {
                if (!byId.containsKey(dep)) {
                    throw new IllegalArgumentException(
                            "case " + tc.id() + " depends on unknown case: " + dep);
                }
                dependents.computeIfAbsent(dep, k -> new HashSet<>()).add(tc.id());
            }
            remaining.put(tc.id(), deps.size());
        }

        List<String> ready = new ArrayList<>();
        for (TestCase tc : cases) {
            if (remaining.get(tc.id()) == 0) {
                ready.add(tc.id());
            }
        }
        ready.sort(Comparator.naturalOrder());

        List<TestCase> out = new ArrayList<>(cases.size());
        while (!ready.isEmpty()) {
            String id = ready.remove(0);
            out.add(byId.get(id));
            for (String child : dependents.getOrDefault(id, Set.of())) {
                int left = remaining.merge(child, -1, Integer::sum);
                if (left == 0) {
                    ready.add(child);
                    ready.sort(Comparator.naturalOrder());
                }
            }
        }
        if (out.size() != cases.size()) {
            List<String> stuck = remaining.entrySet().stream()
                    .filter(e -> e.getValue() > 0)
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
            throw new IllegalArgumentException("dependency cycle detected involving: " + stuck);
        }
        return out;
    }

    private static long mix(long x) {
        // splitmix64 finalizer: deterministic, well-distributed
        x += 0x9E3779B97F4A7C15L;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        return x ^ (x >>> 31);
    }
}
