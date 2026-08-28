# Repository-base-bound interval-policy dry-run proposals

## Purpose

G33 records the exact source changes and validation plan that would be required
to implement a revised monitoring-policy candidate. It is created only after a
G32 disposition records that the revised candidate is suitable for a separate
implementation proposal.

The proposal does not apply a patch or change the checked-in active policy.

## Evidence chain

```text
G31 reviewed-candidate sensitivity result
        ↓
G32 named disposition: suitable_for_separate_implementation_proposal
        ↓
G33 repository-base-bound dry-run proposal
```

The proposal reopens the G32 disposition and retained G31 manifest/summary, then
independently verifies the G29 package against the G27 decision and G26
sensitivity summary.

## Repository binding

The worktree must be clean. The proposal records:

- repository full name;
- exact `HEAD` commit and tree SHAs;
- canonical active-policy source path;
- SHA-256 of that source file;
- complete active and proposed policy snapshots and digests;
- exact threshold differences;
- intended source paths; and
- required focused and complete validation commands.

Any repository commit, tree, source-file, candidate, or upstream evidence change
requires a new proposal.

## Usage

```bash
python -m forecasting.run_interval_policy_dry_run_proposal \
  --disposition evidence/g32.json \
  --revision-sensitivity-summary evidence/g31-summary.parquet \
  --revision-sensitivity-manifest evidence/g31-manifest.json \
  --revision-package evidence/g29-package.json \
  --source-decision evidence/g27-decision.json \
  --source-sensitivity-summary evidence/g26-summary.parquet \
  --repository-root . \
  --repository-full-name alexmarinos87/weather-energy-demand-pipeline \
  --proposed-by "Named engineer" \
  --proposer-role "Data Platform Owner" \
  --proposal-ticket GOV-133 \
  --rationale "Prepare an exact dry-run diff for independent review." \
  --output-dir evidence/g33
```

## Authority boundary

`dry_run_compatible_for_separate_code_review` means only that the retained
configuration is valid against the current repository base. It does not
authorise or apply implementation, update the active policy, activate
thresholds, recalibrate an interval, change a model or schedule, deliver an
alert, deploy, or publish externally.
