# Silver transformation implementations

## Canonical paths

The repository deliberately has no second SQL implementation of silver cleaning or deduplication.

Local development uses:

```text
clean_weather.py
clean_energy.py
```

The canonical Microsoft Fabric path uses:

```text
fabric/notebooks/02_bronze_to_silver.py
```

These implementations own parsing, typing, source-area provenance, UTC timestamps, deterministic identity, and deduplication behaviour. Maintaining blank or parallel SQL files beside them would imply an unsupported execution path and create a future drift surface.

The SQL analytics endpoint files under `fabric/sql/` are pass-through analyst views only. They do not reimplement bronze-to-silver transformations.

## Change rule

When silver semantics change:

1. update the local Python transformation;
2. update the Fabric Spark notebook where parity is required;
3. update shared contracts and tests; and
4. do not add a second SQL transformation unless it becomes a separately reviewed, executable product path with its own parity tests and operating documentation.
