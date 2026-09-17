"""Resolve latest local silver records without inventing a conflicting winner."""
import pandas as pd


def select_latest_records(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Select cleaned canonical rows; filename breaks provenance ties only.

    Existing null business-key grouping is retained. Equality is canonical pandas
    column equality after the caller's conversions, not raw JSON-byte identity.
    """
    if frame.empty:
        return frame.copy()
    latest = frame.groupby(keys, dropna=False, sort=False, observed=True)[
        "ingestion_timestamp_utc"
    ].transform("max")
    candidates = frame.loc[frame["ingestion_timestamp_utc"].eq(latest)]
    compared = [column for column in frame.columns if column != "source_file"]
    distinct = candidates.drop_duplicates(subset=compared)
    conflicting = distinct.duplicated(subset=keys, keep=False)
    if conflicting.any():
        files = sorted(set(distinct.loc[conflicting, "source_file"].astype(str)))
        names = ", ".join(files[:10])
        suffix = f" (and {len(files) - 10} more)" if len(files) > 10 else ""
        raise ValueError(f"Conflicting latest silver records from: {names}{suffix}.")
    return (
        candidates.sort_values(["ingestion_timestamp_utc", "source_file"], kind="mergesort")
        .drop_duplicates(subset=keys, keep="last")
        .reset_index(drop=True)
    )
