"""Publish a local silver batch without replacing existing partition files."""
from datetime import date, datetime
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import pandas as pd


def _partition_day(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("event_date_utc must contain non-null YYYY-MM-DD strings.")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("event_date_utc must contain valid calendar dates.") from exc
    return value


def _require_absent(path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise FileExistsError(f"Refusing to overwrite existing silver output: {path}")


def _rollback(owned: dict[Path, tuple[int, int]], error: BaseException) -> None:
    for path, identity in reversed(list(owned.items())):
        try:
            current = path.lstat()
            if (current.st_dev, current.st_ino) == identity:
                path.unlink()
        except FileNotFoundError:
            pass
        except OSError as cleanup_error:
            error.add_note(f"Could not roll back silver output {path}: {cleanup_error}")


def write_silver_partitions(
    frame: pd.DataFrame,
    output_path: Path,
    *,
    dataset: str,
    run_timestamp: str,
) -> list[Path]:
    """Stage all partitions, then publish complete files without replacement.

    The caller supplies canonical rows and a second-resolution UTC run label.
    Ordinary failures attempt inode-aware cleanup; this is not crash-atomic
    across partitions and requires a trusted filesystem supporting hard links.
    """
    if dataset not in {"energy", "weather"}:
        raise ValueError("dataset must be energy or weather.")
    if not isinstance(run_timestamp, str) or not re.fullmatch(r"[0-9]{8}_[0-9]{6}", run_timestamp):
        raise ValueError("run_timestamp must be YYYYMMDD_HHMMSS.")
    datetime.strptime(run_timestamp, "%Y%m%d_%H%M%S")
    if frame.empty:
        return []
    if not frame.columns.is_unique:
        raise ValueError("Silver output must not contain duplicate columns.")
    if "event_date_utc" not in frame:
        raise ValueError("Silver output requires event_date_utc.")
    # Validate every row before groupby can silently omit a missing partition key.
    frame["event_date_utc"].map(_partition_day)
    partitions = list(frame.groupby("event_date_utc", sort=True, observed=True))
    root = Path(output_path)
    outputs = [root / f"dt={day}" / f"{dataset}_clean_{run_timestamp}.parquet"
               for day, _ in partitions]
    for destination in outputs:
        _require_absent(destination)
    root.mkdir(parents=True, exist_ok=True)
    owned: dict[Path, tuple[int, int]] = {}
    try:
        with TemporaryDirectory(prefix=".silver-stage-", dir=root) as directory:
            staged: list[Path] = []
            for index, (_, partition) in enumerate(partitions):
                # A non-Parquet extension keeps partial staging out of *.parquet scans.
                source = Path(directory) / f"{index:08d}.part"
                with source.open("xb") as handle:
                    partition.to_parquet(handle, index=False)
                staged.append(source)
            for source, destination in zip(staged, outputs):
                destination.parent.mkdir(parents=True, exist_ok=True)
                identity = source.stat()
                # Record before linking so an interrupt immediately after creation is covered.
                owned[destination] = (identity.st_dev, identity.st_ino)
                try:
                    destination.hardlink_to(source)
                except FileExistsError as exc:
                    raise FileExistsError(
                        f"Refusing to overwrite existing silver output: {destination}"
                    ) from exc
    except BaseException as error:
        _rollback(owned, error)
        raise
    return outputs
