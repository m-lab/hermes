---
name: validate-data-pipeline-changes
description: Validate analytical data-pipeline changes before staging, production deployment, historical reruns, or performance claims. Use for SQL or Python workflows that change grouping keys, geolocation or mapping providers, output schemas, statistical granularity, datasets, append-only outputs, lookback logic, warehouse cost identity, batch utilities, backfills, or scheduler configuration.
---

# Validate Data Pipeline Changes

**Created by Loqman Salamatian / [GitHub](https://github.com/Burdantes)**

Use this workflow to prove that a pipeline change is semantically correct,
environment-safe, causally evaluated, and operationally recoverable.

**Licence:** This skill is released under the Creative Commons Attribution 4.0
International licence. You may share and adapt it with attribution.

**Feedback & Support:** Report methodology issues through the skill author's
public GitHub profile. Distinguish a weakness in this workflow from an agent
failing to follow it.

## Establish the change contract

Before editing or running anything:

1. State the single change being tested and the intended improvement.
2. Define success metrics for correctness, cost, runtime, coverage, and
   statistical quality before seeing the result.
3. Record the input dates, code revision, algorithmic regime, dataset, output
   tables, authenticated principal, and billing project.
4. Identify the mutation boundary: read-only validation, staging writes,
   production writes, or historical replacement.
5. Stop if the required principal or payer is not active. Never substitute
   another billing project merely because it has permission.

## Inventory data ownership and persisted state

Classify every referenced table or artifact as:

- mutable operational state;
- environment-local input;
- shared read-only reference;
- primary output;
- secondary output;
- append-only or cross-partition derivative.

Retarget only environment-scoped objects. Preserve deliberately shared
references, and prove that staging SQL contains no production writes.

For reruns, inspect every resume predicate and delete/replacement rule. A
regime-changing rebuild must recompute or explicitly version every derivative;
the ordinary primary-output delete set is not sufficient evidence.

## Audit analytical identity end to end

Start from the first table where the canonical identity can diverge from a
display label.

1. Find every downstream reader, including SQL, Python, serialized identifiers,
   dashboards, and compatibility views.
2. Classify each use as identity, display, provenance, or rollup.
3. Require joins, groups, partitions, thresholds, arrays, windows, and
   serialized keys to use the canonical identity.
4. Search repository-wide for the superseded key in identity operations; do
   not restrict the search to the expected change list.
5. Test semantic output shapes independently: country, state, city, rollup,
   counts, and other separately modelled fields must not be silently combined.

Name endpoints by stable domain roles such as client and server. Confine
direction-dependent source/destination aliases to legacy adapters. When
reviewing directional metrics, expand the formula into origin, destination,
outward leg, return leg, hop order, and observed quantity before declaring
different endpoint aliases inconsistent.

Keep grouping shape and mapping provenance in separate fields. A label such as
`city` or `metro` must not imply a provider, snapshot, or confidence source.

## Place statistical changes before aggregation

Apply a new grouping key to raw observations before eligibility gates,
sampling, trimming, baselines, arrays, windows, and hypothesis tests. Relabeling
or combining already-tested groups is not equivalent to recomputing the
statistics on pooled observations.

Audit every later raw-data reattachment and summary for the same resolver.
Validate the new regime side by side with the old one; do not describe a
metadata-only rewrite as a statistical migration.

## Build a controlled comparison

Preserve the baseline with a snapshot, warehouse time travel, or an immutable
table. Hold constant:

- input date and source population;
- dataset and table versions;
- code revision except for the tested component;
- grouping and threshold regime;
- output selection and downstream filters.

Report traffic-weighted totals, per-group distributions, tail behavior, and
output-key overlap. Separate operational success, resource efficiency, and
statistical quality. Do not claim causality until a confounder audit confirms
that only the intended dimension changed.

## Rehearse before mutation

Render every generated query with its complete execution context. Dry-run it
against the target environment and record bytes processed, estimated cost,
partition scope, principal, and payer.

For batch utilities, assert that all required parameters are substituted before
submission. Exercise at least one failure path and require the process to exit
nonzero if any worker fails; error logs paired with exit status zero are a
failed validation.

## Decide historical impact from evidence

Translate the behavioral difference between old and new code into the narrowest
warehouse predicate possible. Dry-run with a byte cap, count affected rows, and
use that result to decide whether history needs rebuilding.

For bounded historical ranges, inspect outputs whose identity spans partitions
or uses a lookback. Validate uniqueness across both the beginning and end of
the range. Rebuild the affected boundary window together or perform a scoped,
audited cleanup and prove the duplicate count returns to zero.

Apply compatibility or regime guards only at mutation boundaries. A completed
historical partition that will be skipped must not block an unrelated missing
day; require explicit delete/rebuild authorization when an existing partition
will actually change.

## Restore production safely

Treat manual recovery and scheduled execution as separate configurations.
Before re-enabling a scheduler:

1. Inspect its last result and current command.
2. Compare worker count, resource limits, environment, credentials, and
   arguments with the command demonstrated to succeed.
3. Preserve a rollback copy before changing service configuration.
4. Reload the service manager and clear stale failure state.
5. Verify the timer is enabled and scheduled while the service is cleanly idle.

An active timer proves only that a trigger exists; it does not prove the next
command is safe.

## Deliver an evidence report

Before declaring success, report:

- the isolated change and frozen comparison dimensions;
- identity and output-contract checks;
- staging and no-production-write evidence;
- principal, billing project, partitions, bytes, and cost;
- weighted, distributional, and overlap results;
- every primary, secondary, append-only, and lookback output handled;
- affected-row and boundary-duplicate counts;
- batch exit behavior and scheduler configuration;
- unresolved risks and the exact rollback path.

Do not call the change an improvement if the evidence supports only successful
execution or lower cost.
