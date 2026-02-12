import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


class ContractValidationError(ValueError):
    """Raised when a payload fails a data contract."""


@lru_cache(maxsize=8)
def _get_validator(contract_path: str) -> Draft202012Validator:
    with Path(contract_path).open("r") as f:
        schema = json.load(f)

    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def validate_payload(
    payload: dict[str, Any],
    contract_path: Path,
    dataset_name: str,
) -> None:
    """Validate payload against a JSON Schema contract and raise on failure."""
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
