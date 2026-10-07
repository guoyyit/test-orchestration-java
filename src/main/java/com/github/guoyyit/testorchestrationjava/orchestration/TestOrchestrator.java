package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.audit.AuditReport;
import com.github.guoyyit.testorchestrationjava.orchestration.audit.AuditReporter;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestExecutor;
import com.github.guoyyit.testorchestrationjava.orchestration.flaky.FlakyRegistry;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import com.github.guoyyit.testorchestrationjava.orchestration.run.OrchestrationRun;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlan;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlanner;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Public entry point. Plans deterministic shards for the given cases and runs
 * them with dependencies, resource quotas, retries, flaky isolation, crash
 * recovery and an atomic audit trail.
 */
public final class TestOrchestrator {

    private final OrchestrationConfig config;
    private final FlakyRegistry flakyRegistry;
    private final AuditReporter auditReporter = new AuditReporter();
    private final AtomicReference<OrchestrationRun> activeRun = new AtomicReference<>();

    public TestOrchestrator(OrchestrationConfig config) {
        this(config, new FlakyRegistry());
    }

    public TestOrchestrator(OrchestrationConfig config, FlakyRegistry flakyRegistry) {
        validate(config);
        this.config = config;
        this.flakyRegistry = flakyRegistry == null ? new FlakyRegistry() : flakyRegistry;
    }

    private static void validate(OrchestrationConfig c) {
        if (c == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (c.unitCount() <= 0) {
            throw new IllegalArgumentException("unitCount must be positive");
        }
        if (c.maxConcurrency() <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        if (c.maxConcurrency() < c.quarantineConcurrency()) {
            throw new IllegalArgumentException(
                    "quarantineConcurrency must not exceed maxConcurrency");
        }
        if (c.maxRetries() < 0 || c.flakyMaxRetries() < 0) {
            throw new IllegalArgumentException("retry counts must be >= 0");
        }
        if (c.flakyThreshold() <= 0) {
            throw new IllegalArgumentException("flakyThreshold must be >= 1");
        }
    }

    /** Plans shards for inspection without executing (pure function of inputs). */
    public ShardPlan plan(List<TestCase> cases) {
        return ShardPlanner.plan(cases, config.unitCount(), config.seed());
    }

    public RunResult run(List<TestCase> cases, TestExecutor executor) {
        return run("run-" + UUID.randomUUID(), cases, executor);
    }

    public RunResult run(String runId, List<TestCase> cases, TestExecutor executor) {
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor must not be null");
        }
        List<TestCase> ordered = ShardPlanner.topologicalOrder(cases);
        ShardPlan shardPlan = ShardPlanner.plan(ordered, config.unitCount(), config.seed());

        OrchestrationRun run = new OrchestrationRun(runId, ordered, executor, config,
                flakyRegistry, shardPlan);
        if (!activeRun.compareAndSet(null, run)) {
            throw new IllegalStateException("another run is currently active on this orchestrator");
        }
        try {
            RunResult result = run.execute();
            flakyRegistry.persist();
            return result;
        } finally {
            activeRun.set(null);
        }
    }

    public void cancel() {
        OrchestrationRun run = activeRun.get();
        if (run != null) {
            run.cancel();
        }
    }

    public FlakyRegistry flakyRegistry() {
        return flakyRegistry;
    }

    public RunResult runWithAudit(String runId, List<TestCase> cases, TestExecutor executor,
                                  Path auditFile) {
        List<TestCase> ordered = ShardPlanner.topologicalOrder(cases);
        ShardPlan shardPlan = ShardPlanner.plan(ordered, config.unitCount(), config.seed());
        OrchestrationRun run = new OrchestrationRun(runId, ordered, executor, config,
                flakyRegistry, shardPlan);
        if (!activeRun.compareAndSet(null, run)) {
            throw new IllegalStateException("another run is currently active on this orchestrator");
        }
        try {
            RunResult result = run.execute();
            flakyRegistry.persist();
            AuditReport report = auditReporter.build(runId, config.seed(), config.unitCount(),
                    result, shardPlan);
            auditReporter.writeJson(report, auditFile);
            return result;
        } finally {
            activeRun.set(null);
        }
    }
}
