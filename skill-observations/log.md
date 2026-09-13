# Skill Observation Log

Observations captured during task-oriented work. Each entry identifies a
potential skill improvement or new skill opportunity.

**Status key:** OPEN = not yet actioned | ACTIONED = skill updated/created |
DECLINED = user decided not to pursue

---

## 2026-08-10 — Group-identity lineage review

### Observation 1: Audit every consumer of a renamed grouping key

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-10
**Session context:** Tracing an immutable anomaly-detection group label through a multi-stage SQL and Python pipeline.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Cross-stage data-lineage verification

**Issue:** A focused regression test passed for the documented stages while sibling consumers of the same enriched table still constructed pair identifiers and aggregations from the superseded display column. The declared stage list therefore gave a false sense of end-to-end coverage.

**Suggested improvement:** Create a reusable audit workflow that starts from the authoritative grouping-key definition, inventories every downstream reader of the first table where the key can diverge, classifies each use as identity, display, or rollup, and verifies all identity operations plus cross-language handoffs use the immutable key. Include a negative repository-wide search for old-key use in `GROUP BY`, joins, partitions, and serialized identifiers.

**Principle:** A grouping-key migration is complete only when every downstream consumer and serialized representation is audited; tests scoped to the expected change list cannot detect omitted consumers.

### Observation 2: Classify shared references before dataset retargeting

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-10
**Session context:** Making a multi-stage BigQuery workflow dataset-aware for a staging rehearsal.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Safe staging and environment isolation

**Issue:** A blanket dataset-name rewrite correctly redirected operational tables to staging but also redirected a deliberately shared, read-only reference table. The generated SQL remained syntactically plausible and failed only during the staging dry-run because that reference was not cloned.

**Suggested improvement:** Require an explicit inventory of mutable operational tables, environment-local inputs, and shared read-only references before retargeting SQL. Rewrite from an allowlist of environment-scoped tables, leave shared references unchanged, enforce a no-production-writes gate, and dry-run every generated query against the target environment.

**Principle:** Environment retargeting should follow table ownership and mutability, not textual dataset membership; shared reference data and operational state often have different deployment boundaries.

### Observation 3: Preserve separately modelled location fields at presentation boundaries

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-10
**Session context:** Repairing a dashboard-facing public-table projection after a staging identity migration.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Output-contract verification

**Issue:** A final projection combined country and state into the state field even though country was already emitted separately. The query completed and identity checks passed, but the dashboard received values such as a country prefix plus a state name instead of the documented state value.

**Suggested improvement:** Extend output-contract tests and staging acceptance checks to assert semantic field shapes, not only join keys and nullability. For location fields, verify country, state, city, and rollup values independently and include representative display-value samples.

**Principle:** A pipeline can preserve its analytical identity while still breaking consumers through presentation-field reshaping; every separately modelled output field needs its own contract test.

### Observation 4: Move granularity changes ahead of statistical aggregation

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-11
**Session context:** Designing a change from city-keyed to metro-keyed anomaly detection in a multi-stage measurement pipeline.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Statistical migration design

**Issue:** Relabelling or combining city-level anomaly outputs into metros preserves neither the pooled measurement distribution nor the eligibility, trimming, sample counts, baselines, or hypothesis tests that a true metro-keyed detector would produce. Downstream rows can look metro-granular while retaining city-granular statistics.

**Suggested improvement:** Require any grouping-granularity migration to place the new canonical key on raw observations before all windows, sampling, thresholds, arrays, and statistical tests. Audit every later raw-data reattachment and summary for the same resolver, and validate the new regime side by side against the old one rather than treating it as a metadata-only backfill.

**Principle:** A statistical grouping change must occur before the first aggregation; post-hoc aggregation of test outputs is not equivalent to recomputing the test on pooled observations.

### Observation 5: Inventory secondary and append-only outputs before regime reruns

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-11
**Session context:** Adding a selectable statistical grouping regime to a resumable multi-stage data pipeline.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Rerun and deletion safety

**Issue:** The ordinary rerun delete set covered each numbered step's primary output but omitted an append-only derived table and deliberately preserved a secondary output. After changing grouping regime, both would retain results keyed by the old regime even though the primary tables were rebuilt correctly.

**Suggested improvement:** For any algorithm-version or grouping-regime rerun, inventory every primary, secondary, and append-only output plus each stage's resume predicate. Provide an explicit complete-regime-reset path, while retaining narrower preservation defaults for ordinary same-regime reruns.

**Principle:** A regime-changing rerun is correct only when every persisted derivative is either recomputed or explicitly versioned; primary-output deletion alone cannot prevent stale mixed-regime state.

## 2026-08-12 — Step-zero performance validation

### Observation 6: Isolate one changed dimension in performance comparisons

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-12
**Session context:** Validating whether an upstream geolocation substitution improved a multi-stage anomaly-detection pipeline.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Counterfactual benchmark design

**Issue:** An initial comparison concluded that the new geolocation source reduced synthetic-coordinate concentration, but it compared the new metro-grouped staging output with an older city-grouped production output. A later same-table time-travel comparison held the date, grouping regime, pipeline version, and dataset constant and showed that the two highlighted metros actually became more concentrated. The global result was mixed: traffic-weighted concentration improved while the median metro and downstream attribution coverage worsened.

**Suggested improvement:** Create a reusable validation workflow that defines success metrics before execution, freezes input date and algorithmic regime, changes only the component under test, preserves the baseline with snapshots or warehouse time travel, and reports both weighted and per-group distributions plus output-key overlap. Require a confounder audit before any improvement claim and distinguish operational success, resource efficiency, and statistical quality.

**Principle:** A performance claim is causal only when the comparison holds every dimension except the tested change constant; aggregate improvements can coexist with regressions for most groups, so report distributional and overlap metrics alongside weighted totals.

### Observation 7: Pin both warehouse principal and billing project

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-12
**Session context:** Validating a canonical warehouse view after a schema migration.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Warehouse cost controls and execution identity

**Issue:** When the intended billing project rejected a validation job under the active cloud identity, the agent fell back to another available project. The query remained technically scoped and inexpensive, but it violated the user's requirement that all warehouse work run as a specific principal and be billed to a specific project.

**Suggested improvement:** Extend the validation pre-flight to verify both the authenticated principal and the billing/job project before every warehouse job. Pass both explicitly to the client or CLI, stop on a mismatch, and never substitute another payer merely because it has job-creation permission. Include principal, payer, partition scope, bytes, and estimated cost in the pre-execution report.

**Principle:** Data-warehouse authorization includes who executes and who pays; cost safeguards are incomplete unless both identities are pinned and verified before submission.

### Observation 8: Name endpoints by stable domain role

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-12
**Session context:** Designing a compatibility view across path measurements whose collection directions differ.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Canonical schema naming

**Issue:** Legacy source/destination names became ambiguous because two path techniques traverse the same endpoints in opposite directions. A consumer could not tell whether “source” meant the client, the server, or the vantage point of a particular path measurement. The user requested client/server terminology instead.

**Reference file:** `src/hermes/sql/queries/create_events_enriched.sql`

**Suggested improvement:** In canonical-schema migrations, name endpoints by stable domain roles such as client/server and name paths by explicit role-to-role direction. Confine source/destination aliases to the compatibility adapter that reads the legacy schema, and include contract tests that prevent those aliases from leaking into the published interface.

**Principle:** Canonical names should follow stable domain identity rather than direction-dependent transport roles; explicit endpoint-to-endpoint path names prevent semantic reversal across measurement techniques.

### Observation 9: Separate grouping shape from mapping provider

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-12
**Session context:** Reviewing a geolocation-source migration before production deployment.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Provenance and regime naming

**Issue:** A pipeline replaced the geography provider upstream for every mode, but retained a mode name that embedded the previous provider. New city-grouped rows would therefore be stored under a label implying they came from the old provider even though the grouping values came from the new one.

**Suggested improvement:** Model grouping granularity (`city`, `metro`, etc.) and mapping provenance (`provider`, snapshot date, score) as independent fields. Before deployment, assert that each output regime label agrees with the actual upstream resolver and include old/new provider cases in contract tests.

**Principle:** Analytical grouping shape and data provenance are orthogonal dimensions; combining them in one regime label makes provider migrations silently mislabel otherwise valid results.

### Observation 10: Apply regime guards only to writable resume paths

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-12
**Session context:** Diagnosing a daily pipeline that failed before processing a missing day.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Resume and idempotency guards

**Issue:** A top-level resume loop validated the requested analytical regime on an already-complete historical partition before skipping it. Because the daily window included both a complete legacy day and a newly missing day, the harmless mismatch on the skipped day aborted the run before it reached the writable day.

**Suggested improvement:** Separate read-only skip decisions from append/rerun decisions. Regime-conflict checks should remain strict for any table/date that may receive rows, while a final partition already known to be complete should be skipped without requiring it to match the current default. Require explicit delete/rebuild when an operator actually requests conversion of an existing date.

**Principle:** Validate compatibility at mutation boundaries; historical state that will not be changed must not block unrelated forward progress.

### Observation 11: Decompose path formulas into directed legs before judging endpoint consistency

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-13
**Session context:** Auditing an apparent source/destination mismatch in a path-distance and RTT validation formula.
**Skill:** New skill candidate: data-pipeline-identity-audit
**Type:** open-source
**Phase/Area:** Directional metric review

**Issue:** Two adjacent metrics intentionally referenced different endpoints: one measured the remaining one-way distance to the path destination, while the other combined the accumulated outward leg with a geodesic return to the measurement origin to form an RTT lower bound. Comparing aliases syntactically, without expanding the formula into directed legs, made the intentional asymmetry look like a widespread endpoint bug.

**Suggested improvement:** Add a directional-metric audit step that identifies the measurement origin, path destination, hop order, accumulated leg, return leg, and observed quantity before comparing endpoint aliases across expressions. Require a short path-leg comment or contract test wherever adjacent metrics legitimately use different endpoints.

**Principle:** Endpoint consistency is semantic, not textual; validate each formula against its directed path legs and measurement vantage point before treating differing aliases as a defect.

### Observation 12: Validate lookback outputs across both rebuild boundaries

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-14
**Session context:** Rebuilding an ordered range of historical partitions while newer production partitions already existed.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Historical rebuild and append-only output verification

**Issue:** A chronological rebuild correctly prevented each new partition from duplicating earlier rebuilt partitions, but it could not look forward into newer partitions that had been produced before the rebuild began. An append-only output using a seven-day lookback therefore retained later copies of keys newly written into the rebuilt range even though every individual daily step succeeded.

**Suggested improvement:** For any rebuild of a bounded date range, inventory outputs whose identity spans partitions or uses a lookback window. Verify uniqueness across both the range start and range end, and either rebuild the affected boundary window together or run a scoped first-writer cleanup that keeps the earliest row and proves the duplicate count returns to zero.

**Principle:** Per-partition idempotency does not guarantee range-level idempotency when outputs use temporal lookbacks; historical rebuild validation must cover both temporal boundaries.

### Observation 13: Restore schedulers by validating the executable configuration

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-14
**Session context:** Returning a production pipeline to nightly operation after a manual one-worker recovery.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Production recovery and scheduler handoff

**Issue:** Re-enabling the timer was not sufficient to complete recovery: the timer was healthy, but its service still carried the three-worker setting whose previous run had been killed for memory exhaustion. The successful manual rebuild had used one worker, so leaving the service unchanged would have recreated the same failure at the next scheduled invocation.

**Suggested improvement:** Make scheduler restoration a pre-flight checklist: inspect the last service result, compare the scheduled command and resource limits with the proven recovery command, update divergent settings with a rollback copy, reload the service manager, clear stale failure state, and verify the timer is enabled, active, and scheduled while the service is cleanly idle.

**Principle:** A scheduler's active state proves only that a trigger exists; production recovery is complete only when the command it will execute matches the configuration that was demonstrated to succeed.

### Observation 14: Translate code predicates into scoped deployment-impact queries

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-14
**Session context:** Deploying a corrected endpoint guard after a large historical production rebuild had already completed.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Production deployment and backfill decisions

**Issue:** The SQL fix changed which endpoint-null condition allowed a derived metric to be computed. Automatically rerunning hundreds of millions of historical rows would have been expensive, while deploying only for future data risked leaving known affected history inconsistent. The changed predicate could be evaluated directly against existing partitions to determine whether any rows were actually affected.

**Suggested improvement:** Before scheduling a historical rerun for a localized transformation fix, express the old-versus-new behavioral difference as a narrow warehouse predicate. Dry-run it with a byte cap, execute it under the required principal and billing project, and use the affected-row count to decide whether a backfill is necessary. Preserve the query result in the deployment evidence.

**Principle:** A code change does not imply a historical data change; quantify the exact affected population before paying for or skipping a backfill.

### Observation 15: Make batch utilities substitute environment context and fail truthfully

**Status:** ACTIONED — Applied to validate-data-pipeline-changes (weekly review 2026-08-15)
**Date:** 2026-08-14
**Session context:** Running a scoped production-table backfill with a generic multi-date rerun utility.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Operational CLI contracts and failure propagation

**Issue:** The rerun utility substituted date and granularity parameters but omitted the dataset parameter required by newer SQL templates. Every warehouse job failed before mutation, yet the multiprocessing wrapper logged the failures and returned exit code zero, making an all-failed backfill appear successful to automation.

**Reference file:** `src/hermes/pipeline/table_rerun.py`

**Suggested improvement:** Require operational batch utilities to derive and inject the complete execution context from validated fully qualified targets, add a rendered-template assertion or dry-run before mutation, and propagate any worker failure to a nonzero process exit. Cover both parameter completeness and aggregate exit behavior with regression tests.

**Principle:** Batch automation is trustworthy only when it renders the same environment context as the primary workflow and its process exit status reflects every worker outcome; logged errors with a zero exit code are operational false positives.

### Observation 16: Detect reused squash-merged branches before publishing

**Status:** ACTIONED — Applied to github-publish-followups (weekly review 2026-08-15)
**Date:** 2026-08-15
**Session context:** Rebuilding a pull request after follow-up commits were added to a feature branch whose earlier commits had already been squash-merged.
**Skill:** github:yeet
**Type:** open-source
**Phase/Area:** Branch strategy and pull-request preflight

**Issue:** The publishing workflow stayed on an existing feature branch without checking whether its earlier commits had already entered the base through a squash merge. Adding follow-up commits to that branch made GitHub compare the original pre-squash history again, producing conflicts even though the genuinely new commits applied cleanly.

**Suggested improvement:** Extend the workflow's branch-strategy step to fetch the current base and detect whether an existing branch was previously merged with rewritten history. If so, create a fresh branch from the current base and cherry-pick only the unmerged commits. Verify the reconstructed range with a three-way merge or compare check before pushing.

**Principle:** After a squash merge, the source branch is not a safe base for follow-up work because commit ancestry no longer matches the target; publish follow-ups from a fresh target-based branch containing only the new changes.

### Observation 17: Triage skill relevance before full-file inventory

**Status:** ACTIONED — Applied to task-observer (weekly review 2026-08-15)
**Date:** 2026-08-15
**Session context:** Running a comprehensive review where most observations were explicitly scoped to data-pipeline workflows but the procedure required loading every custom skill in full.
**Skill:** task-observer
**Type:** open-source
**Phase/Area:** Comprehensive review skill inventory

**Issue:** The review loaded thousands of lines from unrelated creative, academic, and media skills before confirming that only a small subset had plausible overlap with the observations. This consumed substantial context without improving the mapping and increased the chance that relevant evidence would be crowded out.

**Suggested improvement:** Revise Comprehensive Review Step 2 to inventory all skill names, descriptions, ownership, and paths first; fully read only the skills whose metadata plausibly matches an observation. Reserve a full-library content sweep for active cross-cutting principles that genuinely apply to every skill, and record the excluded skills so the review remains auditable.

**Principle:** Comprehensive coverage does not require indiscriminate context loading; metadata-first routing followed by full reads of plausible targets preserves auditability while keeping attention on relevant evidence.

### Observation 18: Protect skill-name prompts from shell expansion

**Status:** OPEN
**Date:** 2026-08-15
**Session context:** Initializing new skills with generated interface metadata whose required default prompts explicitly referenced `$skill-name`.
**Skill:** skill-creator
**Type:** open-source
**Phase/Area:** Skill initialization and interface generation

**Issue:** Passing a required `$skill-name` default prompt through a shell command allowed the shell to expand the dollar-prefixed text as an environment variable. The initializer succeeded but silently wrote a malformed prompt with the skill-name prefix removed, so structural success did not imply valid interface semantics.

**Suggested improvement:** In Skill Creation Process Step 3, require shell-safe argument passing for interface values containing dollar signs, or recommend generating and patching `agents/openai.yaml` without shell interpolation. Extend validation to assert that `interface.default_prompt` contains the literal, exact `$<frontmatter-name>` token.

**Principle:** Generators must validate semantic literals that cross shell boundaries; successful file creation cannot detect interpolation that silently changes required metadata.

## 2026-08-27 — VM runtime verification

### Observation 19: Identify the authoritative runtime before auditing deployed code

**Status:** OPEN
**Date:** 2026-08-27
**Session context:** Comparing a repository's upload-anomaly pipeline with code present on a production VM.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Deployment discovery and source-of-truth verification

**Issue:** The initial audit found an enabled legacy systemd timer and treated its host checkout as the deployed model. The user corrected that the authoritative pipeline runs in an ephemeral Docker container. The container had already completed and was removed by `docker run --rm`, while dated logs and the image contents proved that the newer union pipeline had run successfully. Host services and checkouts therefore gave a plausible but incorrect picture of production.

**Suggested improvement:** Add a deployment-discovery pre-flight to pipeline audits: inventory containers and images (including stopped or ephemeral execution), compose configuration, schedulers, dated logs, image tags or commit labels, and packaged file hashes before selecting a deployed source tree. Classify coexisting runtimes as authoritative, legacy, staging, or manual, and corroborate the classification with the actual output dataset written by a recent successful run.

**Principle:** Deployment audits must identify the executable artifact that produced current outputs, not merely the most visible host service; ephemeral containers can be authoritative even when no container is present at inspection time.

### Observation 20: Directional signals require directional eligibility flags

**Status:** OPEN
**Date:** 2026-08-27
**Session context:** Designing upload-throughput parity in a bidirectional path-tomography pipeline.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Semantic lineage and directional classification

**Issue:** A proposed plumbing change would add a reverse-path upload predicate to a shared anomaly flag consumed by both forward and reverse path analyses. Although the predicate correctly identified degraded uploads, the shared flag would classify forward paths as anomalous too, contaminating directional attribution while appearing mechanically symmetric.

**Suggested improvement:** Extend pipeline-change validation with a directional-signal audit. For every new signal, record the physical direction it measures, trace every shared eligibility or anomaly flag downstream, and split the flag before the first consumer that branches by direction. Add negative contract tests proving a reverse-only signal cannot populate forward-anomalous cohorts, and vice versa.

**Principle:** Signal parity is not achieved by adding a predicate to every shared OR expression; when evidence has direction, eligibility must branch before directional consumers or the new signal will leak into the wrong cohort.

### Observation 21: Gate repeatability on the downstream semantic identity

**Status:** OPEN
**Date:** 2026-08-28
**Session context:** Proving repeatability of a warehouse anomaly detector before promoting a sandbox image.
**Skill:** New skill candidate: data-pipeline-change-validation
**Type:** open-source
**Phase/Area:** Determinism and release validation

**Issue:** A full-row partition hash changed across identical reruns even after explicit random sources were removed. The table contained multiple metadata-bearing rows for some logical anomaly groups, and distributed `FLOAT64` diagnostics varied in low bits. Treating either difference as a model-decision failure obscured the actual release question; treating the unchanged row count as success would have missed real loss-signal drift.

**Suggested improvement:** Define the downstream semantic key and decision fields before building a repeatability gate. Aggregate duplicate physical rows on that key, hash the complete decision tuple without tolerance, and report full-row/diagnostic hashes separately. When the decision hash fails, compare fields by semantic key to isolate the moving signal before changing another component.

**Principle:** Repeatability gates should be exact about model decisions and explicit about diagnostic numeric drift; neither raw physical-row equality nor row counts alone prove semantic determinism.
