package com.github.guoyyit.testorchestrationjava.orchestration.model;

import java.util.List;
import java.util.Set;

/**
 * Declarative description of a test case.
 *
 * @param id unique case identifier within a run
 * @param historicalDurationMillis historical observed duration, used for weighted sharding
 * @param resourceLabels resource labels consumed while running (e.g. "db", "gpu")
 * @param mutexGroups shared mutex group ids; cases sharing a group never run concurrently
 * @param dependsOn ids that must complete with PASSED before this case may start
 */
public record TestCase(
        String id,
        long historicalDurationMillis,
        Set<String> resourceLabels,
        Set<String> mutexGroups,
        List<String> dependsOn
) {
    public TestCase {
        resourceLabels = resourceLabels == null ? Set.of() : Set.copyOf(resourceLabels);
        mutexGroups = mutexGroups == null ? Set.of() : Set.copyOf(mutexGroups);
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
    }

    /** Convenience constructor for cases without constraints. */
    public TestCase(String id, long historicalDurationMillis) {
        this(id, historicalDurationMillis, Set.of(), Set.of(), List.of());
    }
}
