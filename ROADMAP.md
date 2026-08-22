# Incremental product roadmap

The repository is developed in dependency order. Each goal should be delivered as one bounded PR, validated on the latest `main`, and squash-merged only after CI and review checks pass.

| Goal | Outcome | Status after this increment |
| --- | --- | --- |
| G1 | Forecast-versus-observed reconciliation and lead-time quality evidence | Implemented |
| G2 | Explicit target-weather promotion assessment using minimum evidence, weather quality, and paired demand-model improvement | Next |
| G3 | Fabric bronze/silver ingestion parity for normalized forecast weather | Planned |
| G4 | Fabric paired `ridge_weather_lag` versus `ridge_target_weather` execution and Delta evidence | Planned |
| G5 | Forecast-provider drift, coverage, freshness, and reconciliation monitoring | Planned |
| G6 | Reviewed model registration and promotion controls; no unattended promotion | Planned |
| G7 | Retention, compaction, and cost controls for raw forecasts, reconciliation, predictions, and metrics | Planned |
| G8 | GitHub `main` protection and required CI status checks | Repository setting required |

## Dependency rules

- Do not add forecast weather to the canonical Fabric model before G1 and G2 produce sufficient evidence.
- Do not schedule high-frequency forecast ingestion before retention and capacity impact are understood.
- Do not promote a model merely because one aggregate metric improves; require paired rows, adequate history, coverage by area/horizon/lead bucket, and no material regression in protected slices.
- Keep the existing observed-weather baseline available as the control until the target-weather path is proven and operationally supported.
- Keep credentials outside the repository and prohibit live calls in CI.

## Reusable PR workflow

1. Inspect the latest `main`, open PRs, architecture, tests, and unresolved review feedback.
2. Select the highest-value missing dependency, not the largest possible feature.
3. Rebuild the change directly on the latest base and keep it one coherent layer.
4. Verify exact ancestry, changed paths, and documentation boundaries.
5. Run compilation and the complete test suite; diagnose failures rather than bypassing checks.
6. Check comments, reviews, and unresolved threads.
7. Squash-merge after the exact head is green.
8. Verify the merged tree and confirm no unexpected PR remains open.
