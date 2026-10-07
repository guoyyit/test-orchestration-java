package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlan;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlanner;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardPlannerTest {

    @Test
    void sameInputAndSeedProducesSamePlan() {
        List<TestCase> cases = sampleCases();
        ShardPlan a = ShardPlanner.plan(cases, 3, 42L);
        ShardPlan b = ShardPlanner.plan(cases, 3, 42L);
        assertEquals(a, b);
    }

    @Test
    void differentSeedCanChangePlacement() {
        List<TestCase> cases = sampleCases();
        ShardPlan a = ShardPlanner.plan(cases, 4, 1L);
        ShardPlan b = ShardPlanner.plan(cases, 4, 999983L);
        // at least the full assignment sequences should differ somewhere across many equal-weight cases
        assertNotEquals(flat(a), flat(b));
    }

    @Test
    void everyCaseAssignedExactlyOnce() {
        List<TestCase> cases = sampleCases();
        ShardPlan plan = ShardPlanner.plan(cases, 3, 7L);
        Set<String> assigned = new HashSet<>();
        plan.shards().forEach(s -> assigned.addAll(s.caseIds()));
        Set<String> input = new HashSet<>(cases.stream().map(TestCase::id).toList());
        assertEquals(input, assigned);
    }

    @Test
    void balancesHistoricalLoad() {
        List<TestCase> cases = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            cases.add(new TestCase("c" + i, 100));
        }
        ShardPlan plan = ShardPlanner.plan(cases, 4, 0L);
        int[] sizes = plan.shards().stream().mapToInt(s -> s.caseIds().size()).toArray();
        // 10 equal-weight cases over 4 units -> 3,3,2,2
        assertEquals(10, java.util.Arrays.stream(sizes).sum());
        int max = java.util.Arrays.stream(sizes).max().orElseThrow();
        int min = java.util.Arrays.stream(sizes).min().orElseThrow();
        assertTrue(max - min <= 1, "unbalanced shards: " + java.util.Arrays.toString(sizes));
    }

    @Test
    void unknownDependencyIsRejected() {
        TestCase a = new TestCase("a", 1);
        TestCase b = new TestCase("b", 1, Set.of(), Set.of(), List.of("ghost"));
        assertThrows(IllegalArgumentException.class, () -> ShardPlanner.plan(List.of(a, b), 2, 0L));
    }

    @Test
    void dependencyCycleIsRejected() {
        TestCase a = new TestCase("a", 1, Set.of(), Set.of(), List.of("b"));
        TestCase b = new TestCase("b", 1, Set.of(), Set.of(), List.of("a"));
        assertThrows(IllegalArgumentException.class, () -> ShardPlanner.plan(List.of(a, b), 2, 0L));
    }

    @Test
    void withinShardDependenciesPrecedeDependents() {
        TestCase a = new TestCase("a", 50);
        TestCase b = new TestCase("b", 50, Set.of(), Set.of(), List.of("a"));
        TestCase c = new TestCase("c", 50, Set.of(), Set.of(), List.of("b"));
        ShardPlan plan = ShardPlanner.plan(List.of(a, b, c), 1, 0L);
        assertEquals(List.of("a", "b", "c"), plan.shards().get(0).caseIds());
    }

    private List<String> flat(ShardPlan p) {
        List<String> out = new ArrayList<>();
        p.shards().forEach(s -> {
            out.add(s.unitId());
            out.addAll(s.caseIds());
        });
        return out;
    }

    private List<TestCase> sampleCases() {
        List<TestCase> list = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            list.add(new TestCase("t" + i, (i % 5) * 10L + 1));
        }
        return list;
    }
}
