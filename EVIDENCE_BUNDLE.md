# Reproducible candidate evidence bundle and recovery

## Purpose

This layer packages one **approved** target-weather candidate with the exact evidence needed to understand and reproduce its review decision. It then verifies safe recovery into a clean directory.

The bundle does not deploy or activate a model.

```text
deployment_authorized = false
active_model_unchanged = true
source_files_mutated   = false
```

## Required evidence roles

Creation requires exactly one file for each role:

- `promotion_summary`;
- `comparison_predictions`;
- `reconciliation_metrics`; and
- `provider_health_summary`.

The files may be CSV, Parquet, or JSON. Their embedded run IDs must match the approved candidate manifest:

```text
promotion assessment ID
comparison run ID
reconciliation run ID
provider-health monitor run ID
```

The complete verified model-registry history is included automatically. Extra evidence files may be supplied explicitly.

## Deterministic archive

The output is an uncompressed tar archive with deterministic member ordering and fixed tar metadata:

```text
bundle_manifest.json
candidate/<candidate-id>/manifest_vNNN.json
candidate/<candidate-id>/event_vNNN.json
evidence/<role>/<original-relative-path>
evidence/extra/<original-relative-path>
```

Every entry records source-relative path, archive path, size, SHA-256 and role. The bundle ID is derived from candidate identity, source entries, actor, reason and creation timestamp. Repeating creation with the same inputs and timestamp produces byte-identical archives.

Symbolic links, path traversal, duplicate paths, evidence outside the data root, missing roles, mismatched run IDs, unapproved candidates and oversized bundles are rejected.

## Create

```bash
python3 -m forecasting.run_evidence_bundle create \
  --data-root data \
  --candidate-dir data/model-registry/<candidate-id> \
  --promotion-summary data/promotion/target_weather/target_weather_promotion_summary_<id>.parquet \
  --comparison-predictions data/forecasting/weather_comparison_predictions.parquet \
  --reconciliation-metrics data/reconciliation/forecast_weather/forecast_weather_quality_metrics_<id>.parquet \
  --provider-health-summary data/monitoring/forecast_provider/forecast_provider_health_summary_<id>.parquet \
  --actor alexmarinos87 \
  --reason "Create reviewed candidate recovery package"
```

Default output:

```text
data/bundles/<candidate-id>/evidence_bundle_<bundle-id>.tar
```

## Verify

```bash
python3 -m forecasting.run_evidence_bundle verify \
  --bundle data/bundles/<candidate-id>/evidence_bundle_<bundle-id>.tar
```

Verification rejects non-regular tar members, unsafe names, duplicate or unexpected members, changed sizes or hashes, invalid manifests, broken candidate hash chains and evidence whose embedded IDs do not match the candidate.

## Recover

```bash
python3 -m forecasting.run_evidence_bundle recover \
  --bundle data/bundles/<candidate-id>/evidence_bundle_<bundle-id>.tar \
  --destination recovered/<bundle-id> \
  --confirm-bundle-id <bundle-id> \
  --actor alexmarinos87 \
  --reason "Exercise candidate evidence recovery"
```

Recovery writes into a new temporary directory, verifies every recovered entry and candidate revision, writes an immutable recovery event, then atomically renames the directory into place. Existing destinations are never overwritten.

Re-verify later:

```bash
python3 -m forecasting.run_evidence_bundle verify-recovery \
  --destination recovered/<bundle-id>
```

The recovered directory must contain exactly the bundle entries, `bundle_manifest.json`, and one valid recovery event.

## Contracts

- `data-contracts/candidate_evidence_bundle_schema.json`
- `data-contracts/candidate_evidence_recovery_schema.json`

## Boundary

The bundle records the repository commit and tree SHA but does not download source code or contact GitHub. Reproducing a live Fabric run still requires an explicit environment, credentials, source access and human-authorised execution. This package proves evidence integrity and recoverability, not production readiness.
