from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.model_family_scorecard import build_model_family_scorecard


def _read_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported prediction file type: {path}.")


def read_frame(path: Path) -> pd.DataFrame:
    if path.is_file():
        return _read_file(path)
    if not path.is_dir():
        raise FileNotFoundError(f"Prediction path does not exist: {path}.")
    files = sorted(
        [*path.rglob("*.parquet"), *path.rglob("*.pq"), *path.rglob("*.csv")]
    )
    if not files:
        raise FileNotFoundError(
            f"Prediction directory contains no CSV or Parquet files: {path}."
        )
    return pd.concat([_read_file(file_path) for file_path in files], ignore_index=True)


def select_run(frame: pd.DataFrame, selected: str | None, label: str) -> pd.DataFrame:
    if "run_id" not in frame.columns:
        raise ValueError(f"{label} predictions are missing run_id.")
    run_ids = frame["run_id"].fillna("").astype(str).str.strip()
    if selected is not None:
        result = frame.loc[run_ids == selected].copy()
        if result.empty:
            raise ValueError(f"No {label} rows found for run_id={selected!r}.")
        return result
    unique = sorted(set(run_ids) - {""})
    if len(unique) != 1:
        raise ValueError(
            f"{label} predictions contain {len(unique)} run IDs; select one explicitly."
        )
    return frame.loc[run_ids == unique[0]].copy()


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = path.with_suffix(".csv" if output_format == "csv" else ".parquet")
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite {output}.")
    temporary = output.with_suffix(f".tmp{output.suffix}")
    if temporary.exists():
        raise FileExistsError(f"Temporary output already exists: {temporary}.")
    if output_format == "csv":
        frame.to_csv(temporary, index=False)
    else:
        frame.to_parquet(temporary, index=False)
    temporary.replace(output)
    return output


def _summary_markdown(scorecard: pd.DataFrame, pairwise: pd.DataFrame) -> str:
    run_id = str(scorecard.iloc[0]["scorecard_run_id"])
    lines = [
        "# Paired model-family scorecard",
        "",
        f"- Scorecard run: `{run_id}`",
        f"- UTC source run: `{scorecard.iloc[0]['utc_source_run_id']}`",
        f"- UK-local source run: `{scorecard.iloc[0]['uk_local_source_run_id']}`",
        f"- Contract: `{scorecard.iloc[0]['scorecard_contract_version']}`",
        "",
        "All models are evaluated on identical target identities within each "
        "source-area, horizon, split, and origin slice. Lower retained error is "
        "comparative evidence only; it is not model approval or promotion.",
        "",
        "## Test MAE by area and horizon",
        "",
        "| Source area | Horizon | Model | Observations | MAE MW | RMSE MW |",
        "| --- | ---: | --- | ---: | ---: | ---: |",
    ]
    test = scorecard.loc[scorecard["split"] == "test"].sort_values(
        ["source_area", "requested_horizon_minutes", "mae_mw", "model_name"]
    )
    for row in test.itertuples(index=False):
        lines.append(
            f"| {row.source_area} | {int(row.requested_horizon_minutes)} | "
            f"{row.model_name} | {int(row.paired_observation_count)} | "
            f"{float(row.mae_mw):.4f} | {float(row.rmse_mw):.4f} |"
        )
    lines.extend(
        [
            "",
            "## Test comparison with persistence",
            "",
            "| Source area | Horizon | Candidate | MAE improvement MW | Wins | Ties | Losses |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    paired_test = pairwise.loc[pairwise["split"] == "test"].sort_values(
        [
            "source_area",
            "requested_horizon_minutes",
            "mae_improvement_mw",
            "candidate_model_name",
        ],
        ascending=[True, True, False, True],
    )
    for row in paired_test.itertuples(index=False):
        lines.append(
            f"| {row.source_area} | {int(row.requested_horizon_minutes)} | "
            f"{row.candidate_model_name} | "
            f"{float(row.mae_improvement_mw):.4f} | "
            f"{int(row.win_count)} | {int(row.tie_count)} | "
            f"{int(row.loss_count)} |"
        )
    lines.extend(
        [
            "",
            "The scorecard preserves UTC as the target identity. UK-local calendar "
            "fields remain derived features and are identified through their "
            "feature-contract version.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_text(content: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite {path}.")
    temporary = path.with_suffix(f".tmp{path.suffix}")
    if temporary.exists():
        raise FileExistsError(f"Temporary output already exists: {temporary}.")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a paired scorecard across persistence, elapsed seasonal, "
            "UTC-calendar ridge, and UK-local-calendar ridge evidence."
        )
    )
    parser.add_argument("--utc-predictions", type=Path, required=True)
    parser.add_argument("--uk-local-predictions", type=Path, required=True)
    parser.add_argument("--utc-run-id")
    parser.add_argument("--uk-local-run-id")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/forecasting/model_family_scorecard"),
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="parquet"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    scorecard, pairwise = build_model_family_scorecard(
        select_run(
            read_frame(args.utc_predictions), args.utc_run_id, "UTC"
        ),
        select_run(
            read_frame(args.uk_local_predictions),
            args.uk_local_run_id,
            "UK-local",
        ),
    )
    run_id = str(scorecard.iloc[0]["scorecard_run_id"])
    scorecard_path = _write_frame(
        scorecard,
        args.output_dir / f"model_family_scorecard_{run_id}",
        args.output_format,
    )
    pairwise_path = _write_frame(
        pairwise,
        args.output_dir / f"model_family_pairwise_metrics_{run_id}",
        args.output_format,
    )
    summary_path = _write_text(
        _summary_markdown(scorecard, pairwise),
        args.output_dir / f"model_family_summary_{run_id}.md",
    )
    print(f"Wrote scorecard: {scorecard_path}")
    print(f"Wrote pairwise metrics: {pairwise_path}")
    print(f"Wrote summary: {summary_path}")
    print(
        scorecard.loc[scorecard["split"] == "test"][
            [
                "source_area",
                "requested_horizon_minutes",
                "model_name",
                "paired_observation_count",
                "mae_mw",
                "rmse_mw",
                "source_feature_contract_version",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
