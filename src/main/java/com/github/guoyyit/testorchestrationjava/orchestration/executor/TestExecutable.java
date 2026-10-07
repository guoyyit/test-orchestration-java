package com.github.guoyyit.testorchestrationjava.orchestration.executor;

/** The executable body of a test case. */
@FunctionalInterface
public interface TestExecutable {

    /**
     * Runs the test body.
     *
     * @throws AssertionError thrown for a deterministic failure (never retried)
     * @throws TestFailureException thrown for a classified retriable/deterministic failure
     * @throws UnitCrashException thrown to simulate an execution-unit crash
     * @throws InterruptedException thrown cooperatively when the run is cancelled/timed out
     */
    void execute(TestCaseView view, ExecutionContext context) throws Exception;
}
