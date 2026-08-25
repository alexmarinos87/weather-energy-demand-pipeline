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
| G8 | GitHub `main` protection and required CI status checks | External repository setting required; connector write unavailable |
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
| G17 | Four-area deterministic portfolio demo with manifest-bound resource/city identity and cross-area isolation evidence | Implemented |
| G18 | Europe/London calendar fields, explicit GMT/BST evidence, DST-transition tests, local model mode, and pandas/Fabric parity | Implemented |
| G19a | Elapsed-time previous-day and previous-week local baselines with tolerance, coverage, holdout, and rolling-origin evidence | Implemented |
| G19b | Fabric parity for elapsed-time seasonal baseline comparison and independent quality checks | Implemented as an optional manual subflow |
| G20a | Calibration-only prediction intervals using causally available validation residuals, finite-sample ranks, and empirical test coverage/width evidence | Implemented locally |
| G20b | Fabric parity for calibration-only prediction intervals without test-label leakage | Implemented as an optional manual subflow |
| G21a | Paired area-and-horizon model-family scorecards across persistence, seasonal, UTC-calendar ridge, and UK-local-calendar ridge evidence | Implemented locally |
| G21b | Multi-area portfolio-demo integration for independently validated seasonal evidence | Implemented |
| G21c | Multi-area portfolio-demo integration for independently validated interval evidence | Implemented |
| G22 | Interval coverage, width, calibration-history, and freshness monitoring without automatic recalibration | Implemented as advisory local evidence |
| G23 | Fabric parity for advisory interval monitoring over retained interval metrics without automatic recalibration | Implemented as an optional manual subflow |
| G24 | Multi-area portfolio-demo integration for repeated interval-health evidence and advisory operator reporting | Implemented |
| G25a | Reproducible interval-health trend datasets across area, horizon, model, and coverage slices | Implemented locally |
| G25b | Thin-client Markdown, HTML, and notebook reporting over retained interval-health trend datasets | Implemented locally |
| G26 | Human-reviewed interval-monitoring policy sensitivity evidence without automatic threshold, interval, model, schedule, or promotion changes | Implemented locally |
| G27 | Immutable named monitoring-policy review decision without candidate-threshold activation | Next |

Previous unsplit status before PRs #57–#58:

```text
G25 | Reproducible interval-health trend datasets and thin-client reporting across area, horizon, model, and coverage slices | Next
```

The historical line above is retained only to make the roadmap transition from
the former combined goal explicit; the current status is defined by G25a and
G25b in the table.

## Product-quality sequence

The next product-quality work should bind one retained G26 sensitivity run to an
immutable named human decision. The decision may retain the active policy,
reject a candidate, or request a revised candidate, but it must not activate
candidate thresholds, update the checked-in monitoring policy, recalibrate an
interval, refit a model, change a schedule, promote a candidate, or deliver an
alert.

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
- Multi-area demo rows must remain partitioned by source-area/resource/city
  identity; the demo must not prove correctness by aggregating the areas into
  one anonymous series.
- UTC remains the canonical event and target timestamp. UK-local calendar fields
  are derived features only and must expose their timezone and daylight-saving
  offset explicitly.
- UTC and UK-local model runs must use distinct feature-contract versions and
  output names so one cannot silently overwrite or masquerade as the other.
- Seasonal baselines must match target-minus-period by elapsed UTC time with a
  tolerance and coverage contract; fixed row offsets are not acceptable.
- Previous-day and previous-week models must be scored on the same cohort and
  training boundary as persistence and ridge controls.
- Fabric seasonal parity must reuse one retained point-prediction run and may not
  silently retrain or replace the ordinary baseline.
- Prediction intervals must be calibrated on validation labels available before
  the first test feature timestamp; overlapping validation labels and all test
  labels are forbidden from setting interval width.
- Fabric interval parity must consume one retained point-prediction run, reuse
  its final test cohort, and must not fit or refit any point model.
- Interval coverage is retained as empirical evaluation evidence and must not be
  presented as an unconditional guarantee under distribution shift.
- Model-family scorecards must pair exact UTC target identities, preserve
  area/horizon boundaries, require identical control predictions and training
  evidence, and may not convert retained error rankings into model approval.
- Portfolio seasonal evidence must reopen every artifact, retain all four source
  identities, preserve exact 30-minute cadence and zero-offset elapsed
  day/week references, and bind all five scorecard models to one target digest.
- Portfolio interval evidence must use the retained UTC seasonal point run,
  require at least 24 causally available validation labels, preserve 80/90/95%
  finite-sample ranks, retain all four areas and two horizons, and present
  empirical coverage only as retrospective evidence.
- Interval monitoring may warn about calibration, coverage, width, or freshness
  but must not automatically change a radius, model, schedule, or promotion
  state.
- Fabric interval monitoring must reuse retained interval metrics, preserve the
  exact monitoring slice, bound recent/reference history, validate its own
  retained checks and summary independently, and remain optional and manual.
- Repeated portfolio interval-health evidence must bind each synthetic run,
  health check, summary, and operator report into one immutable manifest while
  preserving all source-area identities and no-side-effect flags.
- Portfolio interval-health verification must reopen all four artifacts, retain
  nine complete runs for each healthy/warning/failed scenario, reproduce status
  and check identities from the retained history, and keep every automatic
  action and alert-delivery flag false.
- Interval-health trend datasets must retain every exact monitoring slice,
  preserve the complete run sequence, label recent/reference windows without
  dropping older evidence, and leave drift values null when reference history
  is insufficient.
- Thin-client interval-health reports must read only retained trend datasets,
  preserve the retained monitor status, and must not rerun monitoring logic,
  construct intervals, or apply an independent threshold policy.
- Monitoring-policy sensitivity evidence may compare reviewed candidate
  thresholds, but it must not update the active policy, mutate retained results,
  recalibrate intervals, change models or schedules, promote candidates, or
  deliver alerts.
- A monitoring-policy review decision must bind one retained sensitivity run,
  identify a named reviewer and review ticket, and record only retain, reject,
  or request-revision evidence. It must not activate thresholds or mutate the
  active policy.

## Reusable PR workflow

1. Inspect the latest `main`, open PRs, architecture, tests, and unresolved review feedback.
2. Select the highest-value missing dependency, not the largest possible feature.
3. Rebuild the change directly on the latest base and keep it one coherent layer.
4. Verify exact paths, line counts, ancestry, and unintended regressions.
5. Run compilation and the complete test suite; diagnose failures rather than bypassing checks.
6. Check unresolved review feedback.
7. Squash-merge one dependency layer at a time.
8. Verify the merged tree and confirm that no PRs remain unexpectedly open.
