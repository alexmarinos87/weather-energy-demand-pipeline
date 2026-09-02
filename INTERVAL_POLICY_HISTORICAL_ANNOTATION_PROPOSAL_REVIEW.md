# Historical interval-policy annotation proposal review

## Purpose

G42 records one immutable named human review over a verified G41 historical
annotation proposal. It separates proposal quality and accountability from any
later annotation-storage change.

## Supported decisions

```text
accept_for_separate_annotation_storage_change
    → separate_annotation_storage_change_request_required

reject_historical_annotation_proposal
    → no_further_action_recorded

request_historical_annotation_proposal_revision
    → revised_historical_annotation_proposal_required
```

A revision request must retain at least one explicit requested update. Acceptance
and rejection cannot contain revision instructions.

## Reopened evidence chain

Before recording a decision, the review reopens and verifies:

```text
G41 annotation proposal
    ↓
G39 compatibility review
    ↓
G38 compatibility summary, manifest, and bound artifacts
```

The receipt binds proposal and source-review hashes, compatibility and trend-run
identities, the reviewed annotation count, and SHA-256 digests of the proposed
annotations and requested-action responses.

## Outputs

```text
interval_policy_historical_annotation_proposal_review_<review-id>.json
interval_policy_historical_annotation_proposal_review_<review-id>.md
```

Existing outputs are never overwritten.

Run locally:

```bash
python3 -m forecasting.run_interval_policy_historical_annotation_proposal_review \
  --annotation-proposal data/proposals/proposal.json \
  --compatibility-review data/reviews/review.json \
  --compatibility-summary data/compatibility/summary.parquet \
  --compatibility-manifest data/compatibility/manifest.json \
  --artifact-directory data/compatibility \
  --decision accept_for_separate_annotation_storage_change \
  --reviewer-name "Named Reviewer" \
  --reviewer-role "Data Platform Owner" \
  --review-ticket GOV-143 \
  --rationale "The proposal is suitable for a separate storage-change request." \
  --output-dir data/interval-policy-historical-annotation-proposal-reviews
```

## Acceptance boundary

An accepted G42 review means only that a separate annotation-storage change
request may be prepared. It does not create or authorize that change and does
not apply the annotations.

Every receipt keeps the following classes of authority false:

- annotation-storage authorization or creation;
- historical annotation application or status rewriting;
- source proposal, review, or compatibility-evidence mutation;
- monitoring rerun or policy activation;
- interval recalibration;
- model, Fabric, schedule, promotion, or alert changes;
- deployment; and
- external publication.
