package com.github.guoyyit.testorchestrationjava.orchestration.exec;

/**
 * Callback that runs the actual body of one test case. Throwing any exception
 * (or returning normally after an assertion error) marks the attempt failed.
 */
@FunctionalInterface
public interface TestExecutor {
    void execute(String caseId) throws Exception;
}
