# Internal consistency of compatibility summaries

The scenario summary digest is an evidence-acceptance boundary, not just a generic
hash. `compatibility_summary_sha256` in
`forecasting/_interval_policy_retained_compatibility_common.py` normalizes the
summary and validates relationships between its conclusions before returning the
existing canonical digest. Valid v2 summaries retain the same bytes-to-semantic-
digest rules and resulting digest values; no schema or public signature changes.

`prepare_compatibility_summary` remains the structural/type normalizer. Calling
it alone is not evidence acceptance: consumers must use the digest and manifest
verification boundaries before relying on retained results. Boolean counts are
rejected before normalization can convert them into integers.

## Relationships implied by this comparison

This contract compares a five-point with a three-point recent-coverage-shortfall
**error** threshold. All non-target historical outcomes remain fixed. A slice
status can therefore stay the same or become failed; it cannot improve or acquire
a new warning merely from this tightening.

Changed-slice and newly-failed-slice counts must agree and cannot exceed the slice
count. Zero changed slices require unchanged overall status. A positive change
count requires current status `failed`. When the previous scenario already
failed, at least one previously failed slice must remain unchanged: not every
slice can be newly failed.

The retained monitoring status must match one of the two compared policy
outcomes, and `retained_status_compatibility` must describe that relationship
correctly. Classification is `fully_compatible` for zero changed slices,
`scenario_status_escalation` for an overall status change, or
`slice_change_without_scenario_change` for changes within an already-failed
scenario. The generated `human_review_required` flag must be true exactly when
slice conclusions change. This is the assessment's evidence flag, not a record
that any person has reviewed or approved it.

For example, a two-slice scenario can remain failed while its previously healthy
slice newly fails; that is valid and requires review. A one-slice scenario that
was already failed cannot claim one newly failed slice. Insufficient reference
history can legitimately retain a warning, or escalate from warning to failed
when the tighter coverage rule fails. These valid cases are tested using actual
canonical monitor and trend outputs from synthetic interval histories.

## Enforcement and limits

The manifest builder and verifier already use the summary digest; they now reject
internally contradictory summaries even when the in-memory and saved summary
match each other and all artifact hashes have been recomputed. Existing downstream
review consumers use these boundaries without a new monitoring or review layer.

These checks establish necessary internal consistency, not independent truth.
They do not recalculate slice-level monitoring results, prove that slice counts
match an authenticated source, validate every report sentence, or authenticate
an author. They do not write evidence, fabricate a human decision, change policy
thresholds, or modify publication/CI/deployment behavior. Keep historical files
unchanged and create separately identified corrected evidence when needed.

## Validation

```bash
python -m pytest -q tests/test_interval_policy_summary_consistency.py
```

The regression file checks invalid counts, booleans, impossible statuses,
classification/review-flag mismatches and invalid saved/supplied summaries, as
well as valid warning, escalating and already-failed multi-slice cases. The
caller-inventory test includes the new test's direct evaluator import. Full
constrained repository CI remains required before acceptance/integration.
