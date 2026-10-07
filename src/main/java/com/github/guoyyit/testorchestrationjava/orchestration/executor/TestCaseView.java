package com.github.guoyyit.testorchestrationjava.orchestration.executor;

/** Read-only view of a case definition given to executables. */
public interface TestCaseView {
    String id();
    long historicalDurationMillis();
}
