from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecasting.demand_weather_report import (
    DemandWeatherAnalysisConfig,
    build_demand_weather_analysis,
)
from forecasting.demo import build_demo_feature_frame


def _read_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input file type: {path}.")


def read_frame(path: Path) -> pd.DataFrame:
    if path.is_file():
        return _read_file(path)
    if not path.is_dir():
        raise FileNotFoundError(f"Input path does not exist: {path}.")
    files = sorted(
        [*path.rglob("*.parquet"), *path.rglob("*.pq"), *path.rglob("*.csv")]
    )
    if not files:
        raise FileNotFoundError(
            f"Input directory contains no CSV or Parquet files: {path}."
        )
    return pd.concat([_read_file(file_path) for file_path in files], ignore_index=True)


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = path.with_suffix(".csv" if output_format == "csv" else ".parquet")
    temporary = output.with_suffix(f".tmp{output.suffix}")
    for candidate in (output, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        if output_format == "csv":
            frame.to_csv(temporary, index=False)
        else:
            frame.to_parquet(temporary, index=False)
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def _write_markdown(content: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    for candidate in (path, temporary):
        if candidate.exists():
            raise FileExistsError(f"Refusing to overwrite {candidate}.")
    try:
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a reproducible descriptive demand/weather report without "
            "live source calls."
        )
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path)
    source.add_argument(
        "--demo",
        action="store_true",
        help="Use deterministic credential-free feature data.",
    )
    parser.add_argument("--top-peak-count", type=int, default=10)
    parser.add_argument("--temperature-bin-width-c", type=float, default=5.0)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analytics/demand_weather"),
    )
    parser.add_argument(
        "--output-format", choices=("csv", "parquet"), default="csv"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    frame = build_demo_feature_frame() if args.demo else read_frame(args.input)
    analysis = build_demand_weather_analysis(
        frame,
        config=DemandWeatherAnalysisConfig(
            top_peak_count=args.top_peak_count,
            temperature_bin_width_c=args.temperature_bin_width_c,
        ),
    )
    run_id = str(analysis["overview"].iloc[0]["analysis_run_id"])
    outputs = {
        "overview": _write_frame(
            analysis["overview"],
            args.output_dir / f"demand_weather_overview_{run_id}",
            args.output_format,
        ),
        "hourly_load_profile": _write_frame(
            analysis["hourly_load_profile"],
            args.output_dir / f"hourly_load_profile_{run_id}",
            args.output_format,
        ),
        "temperature_demand_profile": _write_frame(
            analysis["temperature_demand_profile"],
            args.output_dir / f"temperature_demand_profile_{run_id}",
            args.output_format,
        ),
        "peak_demand_events": _write_frame(
            analysis["peak_demand_events"],
            args.output_dir / f"peak_demand_events_{run_id}",
            args.output_format,
        ),
        "markdown_report": _write_markdown(
            analysis["markdown"],
            args.output_dir / f"demand_weather_report_{run_id}.md",
        ),
    }
    for name, path in outputs.items():
        print(f"Wrote {name}: {path}")
    print(
        analysis["overview"][
            [
                "source_area",
                "resource_id",
                "city",
                "observation_count",
                "demand_mean_mw",
                "demand_p95_mw",
                "demand_max_mw",
                "demand_temperature_pearson",
                "demand_humidity_pearson",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
