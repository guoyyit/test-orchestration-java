package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCaseSpec;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlan;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlanner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardPlannerTest {

    private static TestCaseSpec spec(String id, long millis, Set<String> tags) {
        return new TestCaseSpec(id, Set.of(), Set.of(), tags, Duration.ofMillis(millis), false);
    }

    @Test
    void sameInputAndSeedYieldsIdenticalPlan() {
        List<TestCaseSpec> cases = List.of(
                spec("a", 100, Set.of()), spec("b", 100, Set.of()),
                spec("c", 50, Set.of("db")), spec("d", 50, Set.of("db")),
                spec("e", 10, Set.of()), spec("f", 10, Set.of()));
        ShardPlan p1 = new ShardPlanner(42L).plan(cases, 3);
        ShardPlan p2 = new ShardPlanner(42L).plan(cases, 3);
        assertEquals(p1, p2, "same input + same seed must be reproducible");

        // input order must not matter either
        List<TestCaseSpec> shuffled = new ArrayList<>(cases);
        java.util.Collections.reverse(shuffled);
        ShardPlan p3 = new ShardPlanner(42L).plan(shuffled, 3);
        assertEquals(p1, p3, "plan must be independent of input ordering");
    }

    @Test
    void balancesByHistoricalDuration() {
        List<TestCaseSpec> cases = List.of(
                spec("slow-1", 1000, Set.of()), spec("slow-2", 900, Set.of()),
                spec("fast-1", 100, Set.of()), spec("fast-2", 100, Set.of()));
        ShardPlan plan = new ShardPlanner(1L).plan(cases, 2);
        assertEquals(2, plan.shards().size());
        long max = Math.max(plan.shards().get(0).estimatedMillis(), plan.shards().get(1).estimatedMillis());
        long min = Math.min(plan.shards().get(0).estimatedMillis(), plan.shards().get(1).estimatedMillis());
        assertTrue(max - min <= 200, "LPT should balance load, got max=" + max + " min=" + min);
        // every case assigned exactly once
        List<String> all = plan.shards().stream().flatMap(s -> s.caseIds().stream()).sorted().toList();
        assertEquals(List.of("fast-1", "fast-2", "slow-1", "slow-2"), all);
    }

    @Test
    void spreadsSameResourceTagAcrossShards() {
        List<TestCaseSpec> cases = List.of(
                spec("db-1", 100, Set.of("db")), spec("db-2", 100, Set.of("db")),
                spec("db-3", 100, Set.of("db")), spec("db-4", 100, Set.of("db")));
        ShardPlan plan = new ShardPlanner(7L).plan(cases, 2);
        for (ShardPlan.Shard shard : plan.shards()) {
            long sameTag = shard.caseIds().size();
            assertEquals(2, sameTag, "same-tag cases should spread evenly across shards");
        }
    }
}
