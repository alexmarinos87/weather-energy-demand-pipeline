from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecasting.model_registry import (
    load_candidate_history,
    register_candidate,
    transition_candidate,
    write_candidate_revision,
)


def _read_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return pd.DataFrame([payload] if isinstance(payload, dict) else payload)
    raise ValueError(f"Unsupported evidence file type: {path}.")


def _add_transition_arguments(parser: argparse.ArgumentParser, *, reviewer: bool = False) -> None:
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument("--reason", required=True)
    if reviewer:
        parser.add_argument("--reviewer", required=True)
    else:
        parser.add_argument("--actor", required=True)
    parser.add_argument("--review-ticket")
    parser.add_argument("--event-timestamp-utc")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create and verify an immutable human-reviewed target-weather model "
            "candidate history. No command deploys or activates a model."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    register = subcommands.add_parser("register")
    register.add_argument("--promotion-summary", type=Path, required=True)
    register.add_argument("--provider-health-summary", type=Path, required=True)
    register.add_argument("--repository", required=True)
    register.add_argument("--code-commit-sha", required=True)
    register.add_argument("--code-tree-sha", required=True)
    register.add_argument("--candidate-version", required=True)
    register.add_argument("--training-data-boundary-utc", required=True)
    register.add_argument(
        "--feature-contract-version", nargs="+", required=True
    )
    register.add_argument("--forecast-weather-contract-version", required=True)
    register.add_argument("--actor", required=True)
    register.add_argument("--reason", required=True)
    register.add_argument("--created-at-utc")
    register.add_argument(
        "--output-root", type=Path, default=Path("data/model-registry")
    )

    request = subcommands.add_parser("request-review")
    _add_transition_arguments(request)

    decide = subcommands.add_parser("decide")
    _add_transition_arguments(decide, reviewer=True)
    decide.add_argument("--decision", choices=("approved", "rejected"), required=True)

    retire = subcommands.add_parser("retire")
    _add_transition_arguments(retire)

    verify = subcommands.add_parser("verify")
    verify.add_argument("--candidate-dir", type=Path, required=True)
    return parser


def _transition(args: argparse.Namespace, action: str, actor: str) -> int:
    _, _, latest = load_candidate_history(args.candidate_dir)
    updated, event = transition_candidate(
        latest,
        action=action,
        actor=actor,
        reason=args.reason,
        event_timestamp_utc=args.event_timestamp_utc,
        review_ticket=args.review_ticket,
    )
    manifest_path, event_path = write_candidate_revision(
        args.candidate_dir, updated, event
    )
    print(f"Wrote candidate manifest: {manifest_path}")
    print(f"Wrote review event: {event_path}")
    print(
        json.dumps(
            {
                "candidate_id": updated["candidate_id"],
                "candidate_revision": updated["candidate_revision"],
                "candidate_state": updated["candidate_state"],
                "deployment_authorized": updated["deployment_authorized"],
                "active_model_unchanged": updated["active_model_unchanged"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "register":
        manifest, event = register_candidate(
            _read_file(args.promotion_summary),
            _read_file(args.provider_health_summary),
            repository=args.repository,
            code_commit_sha=args.code_commit_sha,
            code_tree_sha=args.code_tree_sha,
            candidate_version=args.candidate_version,
            training_data_boundary_utc=args.training_data_boundary_utc,
            feature_contract_versions=args.feature_contract_version,
            forecast_weather_contract_version=(
                args.forecast_weather_contract_version
            ),
            actor=args.actor,
            reason=args.reason,
            created_at_utc=args.created_at_utc,
        )
        candidate_directory = args.output_root / manifest["candidate_id"]
        manifest_path, event_path = write_candidate_revision(
            candidate_directory, manifest, event
        )
        print(f"Wrote candidate manifest: {manifest_path}")
        print(f"Wrote registration event: {event_path}")
        print(
            json.dumps(
                {
                    "candidate_id": manifest["candidate_id"],
                    "candidate_state": manifest["candidate_state"],
                    "deployment_authorized": False,
                    "active_model_unchanged": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "request-review":
        return _transition(args, "review_requested", args.actor)
    if args.command == "decide":
        return _transition(args, args.decision, args.reviewer)
    if args.command == "retire":
        return _transition(args, "retired", args.actor)
    if args.command == "verify":
        manifests, events, latest = load_candidate_history(args.candidate_dir)
        print(
            json.dumps(
                {
                    "candidate_id": latest["candidate_id"],
                    "candidate_state": latest["candidate_state"],
                    "verified_manifest_revisions": len(manifests),
                    "verified_review_events": len(events),
                    "latest_manifest_hash": latest["manifest_hash"],
                    "deployment_authorized": latest["deployment_authorized"],
                    "active_model_unchanged": latest["active_model_unchanged"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
