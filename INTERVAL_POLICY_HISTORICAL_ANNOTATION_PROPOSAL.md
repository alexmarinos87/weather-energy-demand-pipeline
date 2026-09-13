# Historical interval-policy annotation proposal

## Purpose

G41 converts one verified G39 `request_historical_annotation` review into an
immutable, non-applying proposal. It describes annotations that a separate human
review may consider without rewriting any retained monitoring row or updating an
annotation store.

## Source contract

A proposal requires one complete G39 review and the exact G38 summary, manifest,
and artifact directory that the review binds. The source review must contain:

```text
decision=request_historical_annotation
decision_effect=separate_historical_annotation_proposal_required
follow_up_human_action_required=true
```

Every source safety field must remain false, and the proposal timestamp cannot
precede the review timestamp.

## Annotation scopes

A proposal may contain:

```text
compatibility_run
    explanatory context applying to the complete G38 assessment

scenario
    explanatory context applying to one named scenario retained by G38
```

Each annotation has a stable identifier, explanatory text, and justification.
Scenario annotations must refer to a scenario already present in the reviewed
G38 evidence. Unknown scenarios are rejected.

## Requested-action coverage

Every action requested by the G39 reviewer must have one response in the same
order. Each response names one or more proposed annotation IDs. Every annotation
must be linked to at least one requested action.

This prevents a proposal from silently omitting part of the review request or
adding an unrelated annotation.

## Outputs

```text
interval_policy_historical_annotation_proposal_<proposal-id>.json
interval_policy_historical_annotation_proposal_<proposal-id>.md
```

Existing outputs are never overwritten.

Run locally:

```bash
python3 -m forecasting.run_interval_policy_historical_annotation_proposal \
  --compatibility-review data/reviews/review.json \
  --compatibility-summary data/compatibility/summary.parquet \
  --compatibility-manifest data/compatibility/manifest.json \
  --artifact-directory data/compatibility \
  --proposal-input data/annotation-proposal-input.json \
  --proposed-by "Named Proposer" \
  --proposer-role "Data Platform Owner" \
  --proposal-ticket GOV-142 \
  --rationale "Prepare explanatory annotations for a separate named review." \
  --output-dir data/interval-policy-historical-annotation-proposals
```

The proposal-input file contains:

```json
{
  "annotations": [
    {
      "annotation_id": "non_retroactive_transition",
      "scope": "compatibility_run",
      "scenario": null,
      "annotation_text": "This historical result was produced under the previous five-point policy.",
      "justification": "The annotation explains policy context without altering the retained status."
    }
  ],
  "requested_action_responses": [
    {
      "requested_action": "Prepare an explanatory annotation for the retained compatibility evidence.",
      "response": "The proposed run-level annotation provides the requested context.",
      "annotation_ids": ["non_retroactive_transition"]
    }
  ]
}
```

## Authority boundary

A G41 proposal always requires a separate named human review. It does not:

- apply an annotation;
- update annotation storage;
- rewrite a historical monitor status;
- mutate the source G39 review or G38 evidence;
- rerun monitoring;
- update or activate a policy;
- recalibrate an interval;
- change a model or schedule;
- execute Fabric;
- deliver an alert;
- promote a candidate;
- deploy; or
- publish externally.
