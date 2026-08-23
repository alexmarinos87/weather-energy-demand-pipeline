# Post-pilot closure bundle and recovery

## Purpose

This layer closes one controlled pilot evidence cycle by packaging the complete verified chain and the run evidence referenced by its immutable receipt.

The closure contains:

```text
pilot plan
pilot preflight
single-use human authorization
operator run receipt
post-run assessment
named post-pilot human decision
all receipt-referenced run evidence files
closure manifest
```

It does not connect to Fabric, mutate the candidate registry, reuse authorization, create a schedule, deploy code, activate a model, quarantine source evidence, or delete anything.

## Chain verification

Bundle creation re-verifies every document and requires exact binding across:

- pilot and plan hash;
- authorization and authorization hash;
- receipt and receipt hash;
- assessment and assessment hash;
- post-pilot decision;
- candidate identity and version; and
- external run identity.

Every run evidence file is re-read from one regular non-symlink evidence root. Its current byte size and SHA-256 must exactly match the immutable receipt before it can enter the archive.

## Deterministic archive

The closure is an uncompressed deterministic tar archive. Members are ordered by archive path and use fixed ownership, mode, and timestamp metadata.

```text
data/pilot-closures/<pilot-id>/post_pilot_closure_<closure-id>.tar
```

The archive contains canonical JSON documents under `documents/`, run evidence under `evidence/`, and `closure_manifest.json`.

The manifest records:

- closure, pilot, candidate, authorization, receipt, assessment, decision, and run identities;
- the hashes binding every upstream document;
- creator, review ticket, reason, and UTC creation time;
- each member role, archive path, source-relative path, size, and SHA-256;
- total member count and byte size; and
- the non-mutating safety boundary.

## Verify

```bash
python3 -m forecasting.run_post_pilot_closure verify \
  --bundle data/pilot-closures/<pilot-id>/post_pilot_closure_<closure-id>.tar
```

Verification rejects:

- absolute paths or `..` traversal;
- symbolic links, hard links, devices, FIFOs, or unknown tar members;
- duplicate paths;
- undeclared or missing files;
- size or SHA-256 mismatches;
- manifest hash or identity inconsistencies; and
- archives above the configured member or byte bounds.

## Recover

```bash
python3 -m forecasting.run_post_pilot_closure recover \
  --bundle data/pilot-closures/<pilot-id>/post_pilot_closure_<closure-id>.tar \
  --recovery-root recovered/pilot-closures
```

Recovery creates a new `<closure-id>` directory only. Existing targets are rejected. After extraction, every recovered file is re-hashed and the directory is checked for missing, extra, or symbolic-link entries.

## Create

```bash
python3 -m forecasting.run_post_pilot_closure bundle \
  --plan data/fabric-pilots/<pilot-id>/pilot_plan_v001.json \
  --preflight data/fabric-pilots/<pilot-id>/pilot_preflight_<id>.json \
  --authorization data/fabric-pilots/<pilot-id>/pilot_authorization_<id>.json \
  --receipt data/fabric-pilots/<pilot-id>/pilot_run_receipt_<id>.json \
  --assessment data/fabric-pilots/<pilot-id>/pilot_run_assessment_<id>.json \
  --decision-record data/fabric-pilots/<pilot-id>/post_pilot_decision_<id>.json \
  --evidence-root pilot-output \
  --created-by "Named human reviewer" \
  --review-ticket PILOT-CLOSURE-001 \
  --reason "Close and preserve the complete reviewed pilot evidence cycle"
```

## Contracts

- `data-contracts/post_pilot_closure_manifest_schema.json`
- `data-contracts/post_pilot_closure_verification_schema.json`

## Safety boundary

Every manifest and verification record hard-codes:

```text
active_model_unchanged=true
model_registry_mutation_allowed=false
authorization_reuse_allowed=false
schedule_activation_allowed=false
deployment_authorized=false
model_activation_authorized=false
source_evidence_mutated=false
permanent_deletion_allowed=false
```

## Next dependency

After closure and clean recovery are proven, the next product decision is whether to prepare a new evidence cycle, create a candidate revision, or complete a separately reviewed registry retirement transition according to the recorded human decision.
