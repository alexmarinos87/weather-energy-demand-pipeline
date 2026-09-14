import json
from functools import lru_cache
from math import isfinite
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, SchemaError


class ContractValidationError(ValueError):
    """Raised when a payload fails a data contract or the contract is invalid."""


def _unique_schema_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate schema member: {key!r}.")
        result[key] = value
    return result


def _reject_nonfinite_constant(token: str) -> Any:
    raise ValueError(f"Non-finite schema number: {token}.")


def _finite_schema_float(token: str) -> float:
    value = float(token)
    if not isfinite(value):
        raise ValueError("Schema number exceeds finite floating-point range.")
    return value


@lru_cache(maxsize=8)
def _compile_validator(schema_content: str) -> Draft202012Validator:
    """Cache compilation, never the association between a path and its contents."""
    schema = json.loads(
        schema_content,
        object_pairs_hook=_unique_schema_members,
        parse_constant=_reject_nonfinite_constant,
        parse_float=_finite_schema_float,
    )
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _get_validator(contract_path: str) -> Draft202012Validator:
    """Read current content on every call; unreadable files never use old rules."""
    path = Path(contract_path)
    try:
        content = path.read_text(encoding="utf-8")
        return _compile_validator(content)
    except (ValueError, SchemaError) as exc:
        raise ContractValidationError(
            f"Invalid data contract {path.name}: {exc}"
        ) from exc


def validate_payload(
    payload: dict[str, Any],
    contract_path: Path,
    dataset_name: str,
) -> None:
    """Validate payload against the current JSON Schema contract; never rewrite it."""
    validator = _get_validator(str(contract_path.resolve()))
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.absolute_path))

    if not errors:
        return

    lines = [
        (
            f"{dataset_name} payload failed contract "
            f"{contract_path.name} with {len(errors)} issue(s):"
        )
    ]

    for err in errors[:5]:
        path = ".".join(str(item) for item in err.absolute_path) or "<root>"
        lines.append(f"- {path}: {err.message}")

    if len(errors) > 5:
        lines.append(f"- ... {len(errors) - 5} additional issue(s)")

    raise ContractValidationError("\n".join(lines))
