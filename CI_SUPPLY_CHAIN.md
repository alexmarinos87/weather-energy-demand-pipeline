# CI action supply-chain controls

## Purpose

The CI workflow uses only reviewed GitHub Actions pinned to immutable 40-character commit SHAs. Floating major-version tags are not accepted because they can resolve to different code without a repository change.

## Approved action identities

| Action | Reviewed release | Pinned commit |
| --- | --- | --- |
| `actions/checkout` | `v6.0.2` | `de0fac2e4500dabe0009e67214ff5f5447ce83dd` |
| `actions/setup-python` | `v6.2.0` | `a309ff8b426b58ec0e2a45f0f869d46889d02405` |

Both reviewed action generations use the current Node 24 action runtime. The version comments in `.github/workflows/ci.yml` are informational; the immutable commit is the executable identity.

## Token and credential boundary

The workflow declares:

```yaml
permissions:
  contents: read
```

Checkout additionally uses:

```yaml
persist-credentials: false
```

The test job therefore does not retain repository write credentials after checkout and does not request issue, pull-request, package, deployment, or workflow write permission.

## Reproducibility and resource controls

- Python remains explicit at `3.11`.
- The pip download cache is keyed from both `requirements.txt` and `constraints/ci-python311-linux.txt`.
- Direct requirements and the full Linux/Python 3.11 transitive resolution are installed together.
- `python -m pip check` validates the resolved dependency graph before compilation.
- Dependency installation does not upgrade pip opportunistically.
- Compilation and tests use `python -m` so the selected interpreter owns the invoked modules.
- A 20-minute job timeout bounds stuck CI execution.
- Concurrency cancels an obsolete run for the same workflow and ref.

## Updating an action

An action upgrade requires a bounded PR that:

1. identifies the official release;
2. resolves its exact commit SHA from the official action repository;
3. updates both the workflow and the approved identity test;
4. keeps token permissions and checkout credential persistence unchanged unless a separately reviewed use case requires more privilege;
5. runs the complete repository suite; and
6. records the reviewed release in this document.

## Regression guard

`tests/test_ci_action_provenance.py` rejects:

- floating action tags or branches;
- unknown action identities;
- incorrect approved SHAs;
- stored checkout credentials;
- write permissions;
- missing requirement or constraint cache identity;
- unconstrained dependency installation;
- opportunistic pip upgrades; and
- unbounded or duplicate CI runs.

The dependency closure and refresh procedure are documented separately in `DEPENDENCY_REPRODUCIBILITY.md`.
