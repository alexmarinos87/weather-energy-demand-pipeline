# Python dependency reproducibility

## Scope

`requirements.txt` remains the human-maintained list of direct application and test dependencies. `constraints/ci-python311-linux.txt` pins the complete dependency resolution used by CI on Python 3.11 and Ubuntu 24.04.

The constraints file includes both direct and transitive packages. CI installs with:

```bash
python -m pip install \
  -r requirements.txt \
  -c constraints/ci-python311-linux.txt

python -m pip check
```

This prevents a transitive dependency from changing merely because a newer package was published.

## Current environment identity

The constraint set was captured from the successful commit-pinned Node 24 CI run introduced in PR #41. It covers the resolved packages required by:

- pandas and its NumPy/date/time dependencies;
- PyArrow;
- JSON Schema validation;
- Requests;
- PyYAML; and
- pytest and its runtime dependencies.

It intentionally does not pin pip, setuptools, or wheel because those are interpreter/bootstrap tooling rather than repository runtime dependencies.

## Platform boundary

The file name is explicit:

```text
constraints/ci-python311-linux.txt
```

It is the supported CI resolution, not a claim that the same wheel set is appropriate for every Python version or operating system. Local developers may use the same constraints on compatible Python 3.11 environments. A different supported platform requires a separately named and tested constraint set.

## Refresh procedure

A refresh must be one reviewed PR:

1. start from the current Python 3.11 Ubuntu CI environment;
2. resolve `requirements.txt` without changing application code;
3. record the complete non-bootstrap package set with exact versions;
4. update the constraint file;
5. run `python -m pip check`;
6. run compilation and the complete repository suite; and
7. explain every direct or transitive version change in the PR.

Do not loosen a constraint to solve an unrelated application failure. Diagnose compatibility and update the direct requirement or code contract deliberately.

## Regression guard

`tests/test_dependency_constraints.py` verifies that:

- every requirement and constraint is an exact `==` pin;
- names are unique after canonical normalization;
- every direct requirement has the identical version in the CI constraints;
- the expected transitive closure is present;
- the running CI environment contains each constrained package at the exact recorded version; and
- the workflow uses both the requirements and constraints files plus `pip check`.
