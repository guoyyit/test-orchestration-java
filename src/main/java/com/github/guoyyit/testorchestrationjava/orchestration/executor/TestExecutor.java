package com.github.guoyyit.testorchestrationjava.orchestration.executor;

import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.Map;

/**
 * Resolves the executable body for a test case.
 */
@FunctionalInterface
public interface TestExecutor {

    /**
     * @param cases all cases in the run (identities are stable)
     * @return case id -> executable body; ids without a mapping fail to resolve
     */
    Map<String, TestExecutable> resolve(java.util.List<TestCase> cases);
}
