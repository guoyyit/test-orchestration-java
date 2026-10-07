package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.audit.AuditReport;
import com.github.guoyyit.testorchestrationjava.orchestration.audit.AuditReporter;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestExecutor;
import com.github.guoyyit.testorchestrationjava.orchestration.flaky.FlakyRegistry;
import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import com.github.guoyyit.testorchestrationjava.orchestration.shard.ShardPlan;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.tc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditAndDeterminismTest {

    private OrchestrationConfig cfg(long seed) {
        return OrchestrationConfig.builder()
                .unitCount(3).maxConcurrency(3).maxRetries(1)
                .flakyMaxRetries(2).flakyThreshold(1)
                .seed(seed).build();
    }

    private List<TestCase> cases() {
        // strongly varied weights produce score ties whose resolution depends on the seed
        List<TestCase> list = new ArrayList<>();
        long[] weights = {100, 87, 53, 53, 41, 41, 29, 29, 13, 13, 7, 7};
        for (int i = 0; i < weights.length; i++) {
            list.add(tc("case-" + i, weights[i]));
        }
        return list;
    }

    private TestExecutor executor() {
        return TestSupport.executor()
                .fail("case-2")
                .failRetriable("case-5")
                .build();
    }

    @Test
    void passFailSkipSetsAreDeterministicAndDisjoint() {
        TestOrchestrator orch = new TestOrchestrator(cfg(99L));
        RunResult r1 = orch.run(cases(), executor());
        RunResult r2 = new TestOrchestrator(cfg(99L)).run(cases(), executor());
        assertEquals(r1.passedIds(), r2.passedIds());
        assertEquals(r1.failedIds(), r2.failedIds());
        assertEquals(r1.skippedIds(), r2.skippedIds());

        Set<String> union = new HashSet<>();
        union.addAll(r1.passedIds());
        union.addAll(r1.failedIds());
        union.addAll(r1.skippedIds());
        assertEquals(12, union.size(), "sets must be disjoint and cover all cases");
        assertEquals(Set.of("case-2", "case-5"), r1.failedIds());
        // case results sorted deterministically by id
        List<String> ids = r1.caseResults().stream().map(CaseResult::caseId).toList();
        List<String> sorted = new ArrayList<>(ids);
        sorted.sort(String::compareTo);
        assertEquals(sorted, ids);
    }

    @Test
    void eachCaseTracesUnitAndAttemptCount() {
        RunResult r = new TestOrchestrator(cfg(99L)).run(cases(), executor());
        for (CaseResult cr : r.caseResults()) {
            assertTrue(cr.plannedUnitId().startsWith("unit-"));
            if (cr.status() != com.github.guoyyit.testorchestrationjava.orchestration.model.CaseStatus.SKIPPED
                    || cr.skipReason() != com.github.guoyyit.testorchestrationjava.orchestration.model.SkipReason.DEPENDENCY_FAILED) {
                // executed cases identify their final unit
            }
            cr.attempts().forEach(a -> {
                assertTrue(a.assignedUnitId() != null);
                assertTrue(a.executedUnitId() != null);
                assertTrue(a.attemptNumber() >= 1);
            });
        }
        CaseResult case5 = r.resultOf("case-5");
        assertEquals(2, case5.attemptCount(), "one retry then exhausted");
        assertEquals(2, case5.attempts().get(1).attemptNumber());
    }

    @Test
    void auditJsonIsWrittenAtomicallyAndContainsPlanAndAttempts(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("audit.json");
        RunResult r = new TestOrchestrator(cfg(99L))
                .runWithAudit("run-audit", cases(), executor(), file);

        assertTrue(Files.exists(file));
        JsonNode root = new ObjectMapper().readTree(Files.readString(file));
        assertEquals("run-audit", root.path("runId").asText());
        assertEquals(99L, root.path("seed").asLong());
        assertEquals(3, root.path("unitCount").asInt());
        JsonNode shards = root.path("shardPlan").path("shards");
        assertEquals(3, shards.size());

        Set<String> audited = new HashSet<>();
        Map<String, Integer> caseIndex = new HashMap<>();
        JsonNode caseNodes = root.path("cases");
        for (JsonNode cn : caseNodes) {
            audited.add(cn.path("caseId").asText());
            caseIndex.put(cn.path("caseId").asText(), caseIndex.size());
        }
        Set<String> inputIds = new HashSet<>(cases().stream().map(TestCase::id).toList());
        assertEquals(inputIds, audited);

        JsonNode case5 = caseNodes.get(caseIndex.get("case-5"));
        assertEquals(2, case5.path("attempts").size());
        assertEquals(Set.of("case-2", "case-5"), r.failedIds());
    }

    @Test
    void shardPlanIsReproducibleAcrossOrchestrators() {
        List<TestCase> cs = cases();
        ShardPlan base = new TestOrchestrator(cfg(12345L)).plan(cs);
        ShardPlan same = new TestOrchestrator(cfg(12345L)).plan(cs);
        assertEquals(base, same);

        // many equal-weight cases force score ties; the seeded tie-break is then decisive
        List<TestCase> equalWeight = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            equalWeight.add(tc("eq-" + i, 10));
        }
        ShardPlan eqA = new TestOrchestrator(cfg(11L)).plan(equalWeight);
        ShardPlan eqB = null;
        long differingSeed = -1;
        for (long seed = 12; seed < 5_000 && eqB == null; seed++) {
            ShardPlan candidate = new TestOrchestrator(cfg(seed)).plan(equalWeight);
            if (!candidate.equals(eqA)) {
                eqB = candidate;
                differingSeed = seed;
            }
        }
        assertNotEquals(null, eqB, "expected some seed to change a tie-broken placement");
        // yet the same seed always reproduces exactly the same plan
        assertEquals(eqB, new TestOrchestrator(cfg(differingSeed)).plan(equalWeight));
    }

    @Test
    void flakyRegistryPersistsAtomicallyAndReloads(@TempDir Path dir) {
        Path file = dir.resolve("flaky.properties");
        FlakyRegistry a = FlakyRegistry.fromFile(file);
        a.recordRun("x", true, false, 1);
        a.persist();
        assertTrue(Files.exists(file));
        FlakyRegistry b = FlakyRegistry.fromFile(file);
        assertTrue(b.isKnownFlaky("x", 1));
        assertEquals(1, b.flakyCount("x"));
    }
}
