package com.github.guoyyit.testorchestrationjava.orchestration.result;

import com.github.guoyyit.testorchestrationjava.orchestration.model.RunSummary;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestStatus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe, append-only result store.
 *
 * <p>A case's final {@link TestResult} is committed exactly once, atomically:
 * either the whole record (with its full attempt history) becomes visible, or
 * nothing does. A crash mid-attempt therefore never leaves a partially written
 * result, and the final conclusion of a case is unique.</p>
 */
public class ResultStore {

    private final ConcurrentHashMap<String, TestResult> results = new ConcurrentHashMap<>();

    /**
     * Atomically commit the final result of a case. First commit wins; a second
     * commit for the same case is rejected so the conclusion stays unique.
     *
     * @throws IllegalStateException if a final result was already committed for the case
     */
    public void commit(TestResult result) {
        TestResult existing = results.putIfAbsent(result.caseId(), result);
        if (existing != null) {
            throw new IllegalStateException(
                    "duplicate final result for case " + result.caseId()
                            + ": existing=" + existing.status() + ", rejected=" + result.status());
        }
    }

    public Optional<TestResult> find(String caseId) {
        return Optional.ofNullable(results.get(caseId));
    }

    /** Stable snapshot ordered by case id for deterministic reporting. */
    public List<TestResult> snapshot() {
        return results.values().stream()
                .sorted(Comparator.comparing(TestResult::caseId))
                .toList();
    }

    /**
     * Aggregate committed results into a deterministic summary. The passed,
     * failed and skipped sets are disjoint and sorted. Any submitted case
     * without a committed final result is counted as SKIPPED — never as passed.
     */
    public RunSummary summarize(String runId, List<String> allCaseIds) {
        Set<String> passed = new TreeSet<>();
        Set<String> failed = new TreeSet<>();
        Set<String> skipped = new TreeSet<>();
        Map<String, TestResult> byId = new LinkedHashMap<>();

        List<String> ids = new ArrayList<>(allCaseIds);
        ids.sort(Comparator.naturalOrder());
        for (String id : ids) {
            TestResult r = results.get(id);
            if (r == null) {
                skipped.add(id);
                continue;
            }
            byId.put(id, r);
            switch (r.status()) {
                case PASSED -> passed.add(id);
                case FAILED -> failed.add(id);
                default -> skipped.add(id);
            }
        }
        return new RunSummary(runId, passed, failed, skipped, byId);
    }
}
