# Interval-policy implementation dry run

## Purpose

G33 turns one G32 disposition of
`suitable_for_separate_implementation_proposal` into an immutable,
repository-base-bound description of a possible monitoring-policy source change.

It does not edit the repository and does not authorize a code change.

## Evidence chain

```text
retained G26 sensitivity summary
        ↓
G27 request-revision decision
        ↓
G29 candidate-revision package
        ↓
G30 named package review
        ↓
G31 revised-candidate sensitivity result
        ↓
G32 suitable-for-proposal disposition
        ↓
G33 repository-base-bound dry run
```

The implementation reopens and verifies the G32 disposition and the G29 package
against their retained source evidence before preparing a proposal.

## Repository binding

A proposal records:

- the exact 40-character base commit;
- the exact base Git tree;
- `forecasting/interval_monitoring.py`;
- the source file SHA-256;
- the Git blob SHA-1 for the source file;
- the parsed active policy defaults;
- the active policy digest;
- the complete proposed policy and digest;
- exact field-level source edits and line numbers;
- a unified dry-run patch;
- intended code and test paths; and
- focused and complete validation commands.

Verification fails when the current base commit, base tree, source contents, active
policy defaults, revised-candidate digest, or upstream evidence chain changes.

## Source contract

The dry run parses `PredictionIntervalMonitoringConfig` from the supplied source
file. Every threshold default must be a literal and `policy_version` must continue
to reference `POLICY_VERSION`.

The source defaults must equal the checked-in
`PredictionIntervalMonitoringConfig()` snapshot. This prevents a proposal from
being reviewed against a stale or unrelated source file.

## Validation contract

A proposal must include at least:

```text
python -m compileall ...
python -m pytest ...
```

and must list `forecasting/interval_monitoring.py` among its intended paths.

## Credential-free example

```bash
python3 -m forecasting.run_interval_policy_implementation_dry_run \
  --disposition evidence/g32/disposition.json \
  --revision-sensitivity-summary evidence/g31/summary.parquet \
  --revision-sensitivity-manifest evidence/g31/manifest.json \
  --revision-package evidence/g29/package.json \
  --source-decision evidence/g27/decision.json \
  --source-sensitivity-summary evidence/g26/summary.parquet \
  --policy-source forecasting/interval_monitoring.py \
  --repository-base-commit "$BASE_COMMIT" \
  --repository-base-tree "$BASE_TREE" \
  --prepared-by "Named engineer" \
  --preparer-role "Data platform engineer" \
  --implementation-ticket "GOV-133" \
  --rationale "Prepare an exact non-applying diff for separate human review." \
  --intended-path forecasting/interval_monitoring.py \
  --intended-path tests/test_interval_monitoring.py \
  --validation-command "python -m compileall -q forecasting tests" \
  --validation-command "python -m pytest -q" \
  --output-dir data/interval-policy-implementation-dry-runs
```

Existing outputs are never overwritten.

## Authority boundary

Every proposal fixes all implementation, source mutation, threshold activation,
active-policy update, recalibration, model, schedule, promotion, alert,
deployment, and publication fields to `false`.

`ready_for_separate_code_change_review` means only that the dry-run evidence is
internally consistent with its exact repository base. A separate named review and
a later code-change PR are still required.
