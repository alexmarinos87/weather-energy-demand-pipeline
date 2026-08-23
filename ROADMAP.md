# Incremental product roadmap

The repository is developed in dependency order. Each goal is one bounded PR,
validated on the latest `main`, and squash-merged only after CI and review
checks pass.

| Goal | Outcome | Status after this increment |
| --- | --- | --- |
| G1 | Forecast-versus-observed reconciliation and lead-time quality evidence | Implemented |
| G2 | Human-review-only target-weather promotion assessment combining evidence volume, weather quality, and paired demand-model improvement | Implemented |
| G3 | Fabric bronze/silver ingestion parity for normalized forecast weather | Implemented as an optional manual subflow |
| G4 | Fabric paired `ridge_weather_lag` versus `ridge_target_weather` execution and independent Delta evidence | Implemented as an optional manual subflow |
| G5 | Forecast-provider drift, freshness, snapshot cadence/completeness, and reconciliation monitoring | Implemented as advisory local evidence |
| G6 | Immutable model-candidate registration and explicit human review decisions with no deployment authority | Implemented |
| G7a | Content-hashed evidence inventory and dry-run retention, compaction, quarantine, and cost plan | Implemented |
| G7b | Explicit reversible quarantine and hash-verified restore with no permanent deletion | Implemented |
| G7c | Schema-compatible compacted shadow outputs with source-manifest verification and no source replacement | Implemented |
| G8 | GitHub `main` protection and required CI status checks | Next repository setting; connector support required |
| G9 | Deterministic approved-candidate evidence bundle, safe archive verification, clean recovery, and recovery re-verification | Implemented |
| G10a | Immutable non-production Fabric pilot plan bound to approved candidate, verified bundle, verified recovery, code identity, limits, and rollback | Implemented |
| G10b | Repository/environment preflight and time-bounded human pilot authorization | Implemented |
| G10c | Immutable operator run receipt, limit assessment, rollback verification, and post-run review evidence | Implemented |
| G10d | Controlled live Fabric pilot execution | External human operation required |
| G11 | Immutable named post-pilot decision to continue evidence collection, revise, or retire without automatic action | Implemented |
| G12 | Reproducible user-facing demand/weather analytics report and valid thin-client notebooks | Implemented |
| G13 | Remove misleading zero-byte SQL placeholders and enforce non-empty tracked implementation artifacts | Implemented |
| G14 | Current Node 24-compatible GitHub Actions pinned to reviewed commits with read-only token and no persisted checkout credentials | Implemented |
| G15 | Fully pinned Python 3.11/Linux transitive constraints used by CI and verified against the installed environment | Implemented |
| G16 | Consolidated capability index and one-command credential-free local product demo with a hash-verified manifest | Implemented |

## Dependency rules

- Do not execute a pilot merely because a draft plan exists. A current, single-use human authorization is also required.
- Do not schedule or promote the forecast-weather path merely because
  ingestion, comparison, monitoring, candidate registration, lifecycle, or
  recovery support exists.
- Do not replace the observed-weather baseline; use it as the paired control.
- Require reconciliation, promotion assessment, representative provider-health
  evidence, an explicit approved candidate history, and verified recovery
  before pilot planning.
- An `approved` registry state is evidence approval only; it never authorizes
  deployment or changes the active model.
- A post-pilot decision recommends a next action but cannot mutate the registry,
  authorize another pilot, deploy, schedule, or activate a model.
- Pilot planning records credential reference names only. Credential values must
  remain outside repository evidence.
- A pilot must be non-production, manually executed, time/capacity/row bounded,
  unscheduled, and expected to leave `ridge_weather_lag` active.
- Lifecycle planning is non-mutating. Quarantine requires exact plan
  confirmation, current content-hash verification, named authority, and an
  immutable manifest.
- Quarantine is reversible and cannot overwrite a source during restore.
  Permanent deletion remains unavailable.
- A staged compacted output is a verified shadow copy only. Sources remain
  authoritative until a separate reviewed replacement workflow binds
  compaction verification to reversible source quarantine.
- Recovery bundles must contain the complete approved candidate history and
  role-bound promotion, comparison, reconciliation, and provider-health
  evidence.
- Unclassified evidence is retained by default, and protected candidate
  histories block lifecycle changes to referenced evidence.
- Keep credentials outside the repository and prohibit live calls in CI.
- Review quota, retention, capacity, failure behaviour, and rollback before
  enabling a recurring forecast trigger.
- Keep comparison predictions and metrics separate from the ordinary baseline
  tables.
- Monitoring may report evidence but must not remediate, deploy, or promote
  automatically.
- User-facing analytics must be reproducible from retained local or exported
  data and must not require live source calls in CI.
- Tracked source, notebook, SQL, and operating-document artifacts must not be
  empty placeholders that imply an implementation which does not exist.
- GitHub Actions must be pinned to reviewed commit SHAs, use the current action
  runtime, retain read-only token permissions, and avoid persisted checkout
  credentials.
- Python dependency resolution used by CI must be reproducible, platform-named,
  exact, compatibility-checked, and refreshed only through a reviewed change.
- The main README must lead with one obvious credential-free product journey
  before exposing advanced evidence-governance controls.
- The portfolio demo is local evidence only and must keep every live-source,
  Fabric, schedule, promotion, and publication side-effect flag false.

## Reusable PR workflow

1. Inspect the latest `main`, open PRs, architecture, tests, and unresolved review feedback.
2. Select the highest-value missing dependency, not the largest possible feature.
3. Rebuild the change directly on the latest base and keep it one coherent layer.
4. Verify exact paths, line counts, ancestry, and unintended regressions.
5. Run compilation and the complete test suite; diagnose failures rather than bypassing checks.
6. Check unresolved review feedback.
7. Squash-merge one dependency layer at a time.
8. Verify the merged tree and confirm that no PRs remain unexpectedly open.
