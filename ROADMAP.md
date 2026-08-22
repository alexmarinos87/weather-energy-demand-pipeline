# Incremental product roadmap

The repository is developed in dependency order. Each goal is one bounded PR, validated on the latest `main`, and squash-merged only after CI and review checks pass.

| Goal | Outcome | Status after this increment |
| --- | --- | --- |
| G1 | Forecast-versus-observed reconciliation and lead-time quality evidence | Implemented |
| G2 | Human-review-only target-weather promotion assessment combining evidence volume, weather quality, and paired demand-model improvement | Implemented |
| G3 | Fabric bronze/silver ingestion parity for normalized forecast weather | Implemented as an optional manual subflow |
| G4 | Fabric paired `ridge_weather_lag` versus `ridge_target_weather` execution and Delta evidence | Next |
| G5 | Forecast-provider drift, coverage, freshness, and reconciliation monitoring | Planned |
| G6 | Reviewed model registration and promotion controls; no unattended promotion | Planned |
| G7 | Retention, compaction, and cost controls for raw forecasts, reconciliation, predictions, and metrics | Planned |
| G8 | GitHub `main` protection and required CI status checks | Repository setting required |

## Dependency rules

- Do not schedule or promote the forecast-weather path merely because bronze/silver support exists.
- Do not replace the observed-weather baseline; use it as the paired control.
- Require G1 reconciliation and G2 promotion evidence before any reviewed model change.
- Keep credentials outside the repository and prohibit live calls in CI.
- Review quota, retention, capacity, and failure behaviour before enabling a recurring forecast trigger.

## Reusable PR workflow

1. Inspect the latest `main`, open PRs, architecture, tests, and unresolved review feedback.
2. Select the highest-value missing dependency, not the largest possible feature.
3. Rebuild the change directly on the latest base and keep it one coherent layer.
4. Verify exact ancestry, changed paths, and documentation boundaries.
5. Run compilation and the complete test suite; diagnose failures rather than bypassing checks.
6. Check comments, reviews, and unresolved threads.
7. Squash-merge after the exact head is green.
8. Verify the merged tree and confirm no unexpected PR remains open.
