"""Lossless file input for the two retained-compatibility commands."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

import pandas as pd


class CompatibilityInputError(ValueError):
    """Raised when an evidence table cannot be interpreted unambiguously."""


def _validate_header(columns: Iterable[object]) -> list[str]:
    names = list(columns)
    if not names or any(not isinstance(name, str) or not name.strip() for name in names):
        raise CompatibilityInputError("Compatibility input header must contain non-empty names.")
    if len(names) != len(set(names)):
        raise CompatibilityInputError("Compatibility input header contains duplicate names.")
    return names


def read_compatibility_frame(path: str | Path) -> pd.DataFrame:
    """Preserve CSV lexemes; existing domain contracts perform type conversion.

    This is a bounded, in-memory local reader, not a concurrent-file snapshot.
    Parquet's explicit types are retained rather than coerced to text.
    """
    source = Path(path)
    suffix = source.suffix.casefold()
    if suffix in {".parquet", ".pq"}:
        frame = pd.read_parquet(source)
        _validate_header(frame.columns)
        return frame
    if suffix != ".csv":
        raise CompatibilityInputError("Compatibility input must be CSV or Parquet.")
    try:
        with source.open("r", encoding="utf-8-sig", newline="") as handle:
            records = csv.reader(handle, strict=True)
            header = _validate_header(next((row for row in records if row), []))
            rows = []
            for row in records:
                if not row:  # Empty physical lines, not records containing empty fields.
                    continue
                if len(row) != len(header):
                    raise CompatibilityInputError(
                        f"Compatibility input row ending on line {records.line_num} "
                        "has a different field count from its header."
                    )
                rows.append(row)
        return pd.DataFrame(rows, columns=header, dtype=str)
    except (csv.Error, UnicodeError) as exc:
        raise CompatibilityInputError(f"Invalid compatibility CSV input: {exc}") from exc
