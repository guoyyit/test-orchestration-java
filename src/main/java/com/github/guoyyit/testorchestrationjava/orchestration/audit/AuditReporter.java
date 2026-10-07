package com.github.guoyyit.testorchestrationjava.orchestration.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptRecord;
import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardAssignment;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlan;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Builds and atomically writes {@link AuditReport}s (deterministic JSON). */
public final class AuditReporter {

    private final ObjectMapper mapper;

    public AuditReporter() {
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public AuditReport build(String runId, long seed, int unitCount,
                             RunResult runResult, ShardPlan plan) {
        return new AuditReport(runId, seed, unitCount,
                runResult.cancelled(), runResult.timedOut(),
                plan, runResult.caseResults());
    }

    /** Serializes to a JSON string with deterministic key ordering. */
    public String toJson(AuditReport report) {
        try {
            return mapper.writeValueAsString(toMap(report));
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize audit report", e);
        }
    }

    /**
     * Writes JSON atomically: temp file in the target directory, then
     * ATOMIC_MOVE (fallback REPLACE_EXISTING). A crash/interrupt at any point
     * leaves either the previous file or the complete new file, never a
     * partially written audit report.
     */
    public void writeJson(AuditReport report, Path targetFile) {
        Path target = targetFile.toAbsolutePath();
        Path parent = target.getParent();
        try {
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = Files.createTempFile(parent, ".audit-", ".json.tmp");
            Files.writeString(tmp, toJson(report));
            try {
                Files.move(tmp, target,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to write audit report: " + target, e);
        }
    }

    private Map<String, Object> toMap(AuditReport r) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("runId", r.runId());
        root.put("seed", r.seed());
        root.put("unitCount", r.unitCount());
        root.put("cancelled", r.cancelled());
        root.put("timedOut", r.timedOut());
        root.put("shardPlan", shardPlanMap(r.shardPlan()));
        root.put("cases", caseMaps(r.caseResults()));
        return root;
    }

    private Map<String, Object> shardPlanMap(ShardPlan plan) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (plan == null) {
            return m;
        }
        m.put("seed", plan.seed());
        List<Map<String, Object>> shards = new ArrayList<>();
        for (ShardAssignment s : plan.shards()) {
            Map<String, Object> sm = new LinkedHashMap<>();
            sm.put("unitId", s.unitId());
            sm.put("caseIds", s.caseIds());
            shards.add(sm);
        }
        m.put("shards", shards);
        return m;
    }

    private List<Map<String, Object>> caseMaps(List<CaseResult> results) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (CaseResult cr : results) {
            Map<String, Object> cm = new LinkedHashMap<>();
            cm.put("caseId", cr.caseId());
            cm.put("status", cr.status().name());
            cm.put("skipReason", cr.skipReason() == null ? null : cr.skipReason().name());
            cm.put("plannedUnitId", cr.plannedUnitId());
            cm.put("finalExecutedUnitId", cr.finalExecutedUnitId());
            cm.put("attemptCount", cr.attempts().size());
            cm.put("flaky", cr.flaky());
            cm.put("quarantined", cr.quarantined());
            List<Map<String, Object>> attempts = new ArrayList<>();
            for (AttemptRecord a : cr.attempts()) {
                Map<String, Object> am = new LinkedHashMap<>();
                am.put("attemptNumber", a.attemptNumber());
                am.put("assignedUnitId", a.assignedUnitId());
                am.put("executedUnitId", a.executedUnitId());
                am.put("status", a.status().name());
                am.put("failureType", a.failureType() == null ? null : a.failureType().name());
                am.put("failureMessage", a.failureMessage());
                am.put("durationMillis", a.durationMillis());
                am.put("interrupted", a.interrupted());
                am.put("crashed", a.crashed());
                attempts.add(am);
            }
            cm.put("attempts", attempts);
            out.add(cm);
        }
        return out;
    }

    /** Convenience: ids present in the audit (used by tests). */
    public static TreeSet<String> caseIds(AuditReport report) {
        TreeSet<String> ids = new TreeSet<>();
        for (CaseResult cr : report.caseResults()) {
            ids.add(cr.caseId());
        }
        return ids;
    }
}
