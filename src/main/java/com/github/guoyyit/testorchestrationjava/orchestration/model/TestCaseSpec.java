package com.github.guoyyit.testorchestrationjava.orchestration.model;

import java.time.Duration;
import java.util.Set;

/**
 * Static declaration of a test case: identity, dependencies, mutual-exclusion
 * groups, required resource tags and historical duration used for sharding.
 */
public record TestCaseSpec(
        String id,
        Set<String> dependsOn,
        Set<String> mutexGroups,
        Set<String> resourceTags,
        Duration historicalDuration,
        boolean flaky) {

    public TestCaseSpec {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("case id must not be blank");
        }
        dependsOn = dependsOn == null ? Set.of() : Set.copyOf(dependsOn);
        mutexGroups = mutexGroups == null ? Set.of() : Set.copyOf(mutexGroups);
        resourceTags = resourceTags == null ? Set.of() : Set.copyOf(resourceTags);
        historicalDuration = historicalDuration == null ? Duration.ZERO : historicalDuration;
    }

    public static TestCaseSpec of(String id) {
        return new TestCaseSpec(id, Set.of(), Set.of(), Set.of(), Duration.ZERO, false);
    }
}
