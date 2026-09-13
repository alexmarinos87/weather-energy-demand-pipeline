# Retained interval-policy compatibility: scenario adapter

## One comparison engine

The canonical G38 engine is `assess_retained_policy_compatibility` in
`forecasting/interval_policy_compatibility.py`. Its direct CLI and three-output
contract are documented in [INTERVAL_POLICY_COMPATIBILITY.md](INTERVAL_POLICY_COMPATIBILITY.md).

This module now adapts that engine's output to the scenario-oriented reporting
and review interface. It is not an independent policy evaluator. The previous
trend-only evaluator has been removed: it used latest-run rather than weighted
recent coverage and re-aged historical observations at assessment time.

## Inputs and historical identity

The adapter requires both the G25a `interval-health-trend-v1` slice trends and
the matching original retained health checks. The checks must retain
`monitor_as_of_utc`; the adapter will not substitute the trend timestamp, the
assessment timestamp, or a guessed reference time.

Every scenario must bind one distinct `monitor_run_id`. The exact monitor/area/
resource/city/horizon/model/feature-contract/coverage/interval-contract slice sets
must match, with no dropped or extra slice. Latest interval identity, recent and
reference counts, calibration history, weighted recent coverage, conditional
drift values, and historical freshness ages must agree. Retained scenario status
must reproduce the original checks under their original three- or five-point
threshold.

The original check table is the comparison authority. Both previous and current
outcomes are delegated to the same engine. Only the reviewed coverage-shortfall
threshold changes; non-target pass/fail outcomes and the original monitoring
reference time remain fixed. Running an assessment a year later does not change
those historical outcomes.

For example, equal-size recent evaluations with coverage 78%, 90%, 90% at a 90%
nominal target have weighted coverage 86% and shortfall four percentage points.
The latest-only shortfall is zero and is not a valid replacement. Actual
`evaluation_observation_count` weights are preserved by the canonical monitor.

## Run

```bash
python -m forecasting.run_interval_policy_retained_compatibility \
  --slice-trends data/interval-health-trends/slice_trends.csv \
  --health-checks data/interval-health/prediction_interval_health_checks.csv \
  --compatibility-run-timestamp 2026-09-13T15:00:00Z \
  --output-dir data/interval-policy-retained-compatibility
```

Both input files must be retained evidence for the same scenarios and exact
slices. The example paths are placeholders for those files, not generated
credentials or live sources. CSV and Parquet are supported.

The Python function retains its name and three-result return shape:

```python
slices, summary, report = evaluate_retained_policy_compatibility(
    slice_trends,
    retained_health_checks=original_checks,
)
```

Omitting `retained_health_checks` raises a migration error. The CLI requires
`--health-checks` before reading inputs or creating output directories.

## Version and migration boundary

New scenario outputs use `interval-policy-retained-compatibility-v2`. The shared
summary validator requires the original monitor run, original as-of timestamp,
semantic source-check SHA-256 and canonical engine contract version. The summary
digest binds these fields; the scenario manifest binds that digest and the exact
output artifact bytes. The direct canonical engine remains
`interval-policy-compatibility-v1`; it is a separate, unchanged contract.

Keep old v1 outputs unchanged. Do not edit their version field or reuse an old
review as approval of a new assessment. Recreate a separate v2 assessment from
original retained checks and obtain a separate human review where needed. If the
original checks are unavailable, the scenario adapter cannot establish the
historical comparison; it fails closed rather than fabricating them.

Public shared helper and manifest module names remain in place for downstream
G39/G40/G41 consumers. Their source fixtures now use canonical monitoring
outputs; production review/ledger/annotation decisions are not created or changed
by this migration.

## Output and publication limits

```text
interval_policy_retained_compatibility_slices_<run-id>.csv|parquet
interval_policy_retained_compatibility_summary_<run-id>.csv|parquet
interval_policy_retained_compatibility_report_<run-id>.md
interval_policy_retained_compatibility_manifest_<run-id>.json
```

The scenario CLI calls `write_retained_compatibility_bundle` in
`forecasting/interval_policy_compatibility_publication.py`. It keeps the existing
arguments, filenames and valid v2 manifest shape. The writer expects the slices,
summary and report returned together by the scenario evaluator; it does not
independently recalculate arbitrary supplied slices or report text.

Before serialization it checks all four final names, including dangling symlinks.
It then writes exclusively into a unique private staging directory on the output
filesystem. All data files are serialized and the manifest builder/verifier checks
the staged bundle before any final filename is exposed. Complete files are
published using no-replace hard links, with the verified manifest **last**. A
concurrent destination collision fails rather than replacing the existing file.
The CLI reports success only after publication returns successfully.

On a serialization or verification failure, no final files are published. On an
ordinary publication failure, cleanup removes only this invocation's file
identities, not unrelated files or a replacement from another writer. Cleanup
continues after an individual removal error and attaches diagnostic notes to the
original exception. Filesystem permission or availability failures can prevent
complete cleanup; inspect any such notes. A newly created output directory may
remain empty after failure.

This requires a local filesystem with hard-link support; unsupported filesystems
fail without an overwriting fallback. No shared sensitivity/JSON writer is changed
by this scenario-specific implementation. Existing evidence produced through
older or other writer functions does not inherit this publication guarantee.

This is **not a crash-atomic four-file transaction** or a power-loss durability
guarantee. A process/host crash can leave partial, unmanifested files or a staging
directory. There is no automatic stale-file cleanup or resume/overwrite mode.
Preserve incomplete evidence for inspection; use a fresh run ID or output directory
for a separate retry. Do not delete files just because a manifest is absent.

Consumers must ignore private staging directories and must verify the final
manifest against all three retained artifacts before consumption. Manifest
existence alone is neither approval nor a substitute for verification. Hashes
detect inconsistent supplied evidence, not coordinated replacement of every
input by an untrusted author. The caller must trust the output directory hierarchy;
this writer is not a lock or sandbox against hostile directory mutation.

The unused **direct-engine** manifest schema was removed after the repository
consumer scan found no Python references. It did not implement a manifest writer.
The implemented **scenario-adapter** manifest schema remains and is versioned v2.
Neither interface claims transactional publication across an entire output set.

## Manifest verification contract

`build_compatibility_manifest` and `verify_compatibility_manifest` enforce the
same contract. Construction verifies the completed manifest before returning it;
it cannot bind a supplied summary to different saved summary content.

Verification requires the existing v2 JSON schema, a valid manifest digest,
matching run and trend identities, and a timezone-aware assessment timestamp
representing the same instant as the summary. Both complete policy snapshots
must exactly match `compatibility_policy_candidates()` from the checked-out
code, including non-target thresholds, candidate versions and metadata. A
recomputed self-hash does not permit a changed or incomplete policy snapshot.
Future policy changes therefore require explicit version/migration review; they
must not reinterpret old evidence under silently altered defaults.

There must be exactly one `slices`, `summary` and `report` entry, with distinct
case-insensitive filenames in one artifact directory. Filenames are exact
basenames, not paths: separators, whitespace aliases and control characters are
rejected. Table artifacts use CSV or Parquet (`.pq` is also accepted), and the
report uses `.md`. Empty files, symlink artifacts and non-regular files are
rejected. The caller controls and must trust the artifact directory hierarchy;
these checks are not a sandbox against hostile concurrent directory changes.

The saved summary is parsed from the same byte snapshot used for its artifact
hash. Its normalized semantic digest must equal the supplied summary digest.
CSV identifiers remain text, duplicate CSV columns are rejected, and row ordering
is normalized by the shared summary contract. Both CSV and Parquet summaries
are supported. Other artifact hashes are streamed, rather than loading complete
slice tables or reports into memory. The saved summary itself is held in memory
for parsing; this is intended for bounded local assessment summaries.

Verification reads files but does not rewrite them, rerun monitoring, or create
a human review. It binds artifact bytes, the expected policy and the summary;
it does **not** independently recompute all slice/report conclusions, establish
authenticity of original source checks, or make publication atomic. The existing
G39 review entry point calls this verifier before recording a decision.

## Validation and authority

Differential regressions compare the adapter with canonical monitoring and the
retained-check engine. They cover historical-time invariance, actual observation
weights, threshold edges, incomplete history, exact multi-area slices, conflicting
source bindings, source immutability, CSV/Parquet output, legacy-version rejection,
and the G39 review/ledger/annotation dependency chain.

Manifest regressions are in `tests/test_interval_policy_manifest_binding.py`.
They exercise rehashed policy changes, timestamp mismatches, unknown fields,
duplicate roles, swapped artifacts, unsafe names, symlinks, empty artifacts,
scattered directories, mismatched saved/supplied summaries, read-only CSV/Parquet
round trips and downstream rejection of an invalid bundle.

Publication regressions are in `tests/test_interval_policy_publication.py`.
They exercise the real CLI with canonical evidence, every output collision,
dangling symlinks, CSV/Parquet serialization failures, manifest rejection,
competing final-name publication, interrupted writes, replacement preservation,
verification before publication and manifest-last ordering. Successful bundles
retain the same four-output contract and do not change the source input files.

All side-effect fields remain false. The assessment does not rewrite historical
statuses, mutate source evidence, rerun monitoring, activate thresholds, change
intervals or models, execute Fabric, schedule a job, deliver an alert, deploy, or
publish externally.
