from __future__ import annotations

import hashlib
import inspect
import json
import re
import subprocess
from dataclasses import asdict, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from forecasting.interval_monitoring import PredictionIntervalMonitoringConfig
from forecasting.interval_policy_candidate_revision import (
    verify_candidate_revision_package,
)
from forecasting.interval_policy_revision_disposition import (
    verify_revision_disposition,
)


PROPOSAL_CONTRACT_VERSION = "interval-policy-dry-run-proposal-v1"
PROPOSAL_STATUS = "dry_run_compatible_for_separate_code_review"
PROPOSAL_ID_PATTERN = re.compile(r"^ipdp-[0-9a-f]{24}$")
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SAFETY_FIELDS = (
    "implementation_authorized",
    "implementation_applied",
    "patch_applied",
    "threshold_activation_authorized",
    "candidate_thresholds_activated",
    "active_policy_updated",
    "source_disposition_mutated",
    "source_package_mutated",
    "source_decision_mutated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)
DEFAULT_TARGET_PATHS = (
    "forecasting/interval_monitoring.py",
    "fabric/notebooks/05f_prediction_interval_monitoring.py",
)
DEFAULT_VALIDATION_COMMANDS = (
    "python -m pip check",
    "python -m compileall -q ingestion transformations forecasting fabric/notebooks tests",
    "python -m pytest -q tests/test_interval_policy_dry_run_proposal.py",
    "python -m pytest -q",
)


class IntervalPolicyDryRunProposalError(ValueError):
    """Raised when a dry-run implementation proposal is unsafe or inconsistent."""


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc(value, "timestamp").isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def _digest(value: Any) -> str:
    payload = json.dumps(
        _canonical(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise IntervalPolicyDryRunProposalError(f"{name} must be non-empty.")
    return text


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyDryRunProposalError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyDryRunProposalError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def _field(document: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in document:
            return document[name]
    raise IntervalPolicyDryRunProposalError(
        "Required evidence field is missing: " + " or ".join(names) + "."
    )


def _call_verifier(function, values: Mapping[str, Any]) -> None:
    """Call an existing verifier by semantic parameter name."""
    arguments = []
    for name in inspect.signature(function).parameters:
        lowered = name.casefold()
        if "disposition" in lowered:
            arguments.append(values["disposition"])
        elif "manifest" in lowered:
            arguments.append(values["manifest"])
        elif "sensitivity" in lowered and "summary" in lowered:
            arguments.append(values["sensitivity_summary"])
        elif "summary" in lowered:
            arguments.append(values["summary"])
        elif "package" in lowered:
            arguments.append(values["package"])
        elif "decision" in lowered:
            arguments.append(values["decision"])
        else:
            raise IntervalPolicyDryRunProposalError(
                f"Unsupported verifier parameter {name!r}."
            )
    function(*arguments)


def _policy_fields() -> tuple[str, ...]:
    return tuple(field.name for field in fields(PredictionIntervalMonitoringConfig))


def _policy_snapshot(document: Mapping[str, Any]) -> dict[str, Any]:
    missing = sorted(set(_policy_fields()) - set(document))
    if missing:
        raise IntervalPolicyDryRunProposalError(
            "Revised candidate is missing policy fields: " + ", ".join(missing) + "."
        )
    snapshot = {name: document[name] for name in _policy_fields()}
    try:
        config = PredictionIntervalMonitoringConfig(**snapshot)
        config.validate()
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyDryRunProposalError(
            "Revised candidate does not satisfy the checked-in policy contract."
        ) from exc
    return asdict(config)


def inspect_repository_base(
    repository_root: Path,
    *,
    active_policy_source_path: str = "forecasting/interval_monitoring.py",
) -> dict[str, str]:
    """Return clean Git and active-source identities for one exact repository base."""
    root = Path(repository_root).resolve()
    if not (root / ".git").exists():
        raise IntervalPolicyDryRunProposalError(
            "repository_root must be a Git worktree."
        )
    try:
        head = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip()
        tree = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD^{tree}"], text=True
        ).strip()
        dirty = subprocess.check_output(
            ["git", "-C", str(root), "status", "--porcelain"], text=True
        ).strip()
    except subprocess.CalledProcessError as exc:
        raise IntervalPolicyDryRunProposalError(
            "Repository identity could not be inspected."
        ) from exc
    if not SHA_PATTERN.fullmatch(head) or not SHA_PATTERN.fullmatch(tree):
        raise IntervalPolicyDryRunProposalError(
            "Repository commit and tree identities must be full Git SHAs."
        )
    if dirty:
        raise IntervalPolicyDryRunProposalError(
            "Dry-run proposals require a clean repository worktree."
        )
    source_path = root / active_policy_source_path
    if not source_path.is_file():
        raise IntervalPolicyDryRunProposalError(
            "Active monitoring-policy source file does not exist."
        )
    return {
        "repository_base_commit_sha": head,
        "repository_base_tree_sha": tree,
        "active_policy_source_path": active_policy_source_path,
        "active_policy_source_sha256": _file_sha256(source_path),
    }


def create_dry_run_implementation_proposal(
    disposition: Mapping[str, Any],
    revision_sensitivity_summary: pd.DataFrame,
    revision_sensitivity_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_root: Path,
    repository_full_name: str,
    proposed_by: str,
    proposer_role: str,
    proposal_ticket: str,
    rationale: str,
    proposed_at_utc: Any | None = None,
    target_paths: Iterable[str] = DEFAULT_TARGET_PATHS,
    validation_commands: Iterable[str] = DEFAULT_VALIDATION_COMMANDS,
) -> dict[str, Any]:
    """Create one repository-base-bound proposal without applying a patch."""
    disposition_document = dict(disposition)
    package_document = dict(revision_package)
    manifest_document = dict(revision_sensitivity_manifest)
    values = {
        "disposition": disposition_document,
        "summary": revision_sensitivity_summary,
        "manifest": manifest_document,
        "package": package_document,
        "decision": dict(source_decision),
        "sensitivity_summary": source_sensitivity_summary,
    }
    _call_verifier(verify_revision_disposition, values)
    _call_verifier(verify_candidate_revision_package, values)

    disposition_name = _field(
        disposition_document,
        "disposition",
        "disposition_decision",
        "revision_disposition",
    )
    if disposition_name != "suitable_for_separate_implementation_proposal":
        raise IntervalPolicyDryRunProposalError(
            "G33 requires a suitable_for_separate_implementation_proposal disposition."
        )

    revised = dict(_field(package_document, "revised_candidate"))
    revised_id = _required_text(revised.get("candidate_id"), "revised candidate ID")
    disposition_candidate = _required_text(
        _field(disposition_document, "revised_candidate_id"),
        "disposition revised candidate ID",
    )
    if revised_id != disposition_candidate:
        raise IntervalPolicyDryRunProposalError(
            "Disposition and revision package target different candidates."
        )

    active_snapshot = asdict(PredictionIntervalMonitoringConfig())
    proposed_snapshot = _policy_snapshot(revised)
    changes = [
        {
            "field": name,
            "active_value": active_snapshot[name],
            "proposed_value": proposed_snapshot[name],
        }
        for name in _policy_fields()
        if active_snapshot[name] != proposed_snapshot[name]
    ]
    if not changes:
        raise IntervalPolicyDryRunProposalError(
            "A dry-run proposal requires at least one policy change."
        )

    targets = tuple(
        dict.fromkeys(_required_text(item, "target path") for item in target_paths)
    )
    if "forecasting/interval_monitoring.py" not in targets:
        raise IntervalPolicyDryRunProposalError(
            "The canonical local monitoring-policy source must be a target path."
        )
    commands = tuple(
        dict.fromkeys(
            _required_text(item, "validation command")
            for item in validation_commands
        )
    )
    if not any("pytest" in command for command in commands):
        raise IntervalPolicyDryRunProposalError(
            "Validation commands must include pytest."
        )
    if not any("compileall" in command for command in commands):
        raise IntervalPolicyDryRunProposalError(
            "Validation commands must include Python compilation."
        )

    repository = inspect_repository_base(repository_root)
    timestamp = _utc(proposed_at_utc or datetime.now(timezone.utc), "proposed_at_utc")
    disposition_timestamp = _utc(
        _field(
            disposition_document,
            "disposition_timestamp_utc",
            "disposed_at_utc",
            "reviewed_at_utc",
        ),
        "disposition timestamp",
    )
    if timestamp < disposition_timestamp:
        raise IntervalPolicyDryRunProposalError(
            "Proposal timestamp cannot precede the G32 disposition."
        )

    diff_manifest = [
        {
            **change,
            "target_paths": list(targets),
            "application_state": "not_applied",
        }
        for change in changes
    ]
    core = {
        "repository_full_name": _required_text(
            repository_full_name, "repository_full_name"
        ),
        **repository,
        "source_disposition_id": _required_text(
            _field(disposition_document, "revision_disposition_id", "disposition_id"),
            "source disposition ID",
        ),
        "source_disposition_sha256": _required_text(
            _field(
                disposition_document,
                "revision_disposition_sha256",
                "disposition_sha256",
            ),
            "source disposition SHA-256",
        ),
        "source_revision_sensitivity_run_id": _required_text(
            _field(
                disposition_document,
                "source_revision_sensitivity_run_id",
                "revision_sensitivity_run_id",
                "sensitivity_run_id",
            ),
            "source revision sensitivity run ID",
        ),
        "source_revision_sensitivity_manifest_sha256": _required_text(
            _field(
                disposition_document,
                "source_revision_sensitivity_manifest_sha256",
                "revision_sensitivity_manifest_sha256",
            ),
            "source revision sensitivity manifest SHA-256",
        ),
        "source_revision_package_id": _required_text(
            _field(package_document, "revision_package_id"),
            "source revision package ID",
        ),
        "source_revision_package_sha256": _required_text(
            _field(package_document, "revision_package_sha256"),
            "source revision package SHA-256",
        ),
        "revised_candidate_id": revised_id,
        "revised_candidate_version": _required_text(
            revised.get("candidate_version"), "revised candidate version"
        ),
        "active_policy_snapshot": active_snapshot,
        "active_policy_sha256": _digest(active_snapshot),
        "proposed_policy_snapshot": proposed_snapshot,
        "proposed_policy_sha256": _digest(proposed_snapshot),
        "changed_thresholds": diff_manifest,
        "changed_threshold_count": len(diff_manifest),
        "target_paths": list(targets),
        "validation_commands": list(commands),
        "proposed_by": _required_text(proposed_by, "proposed_by"),
        "proposer_role": _required_text(proposer_role, "proposer_role"),
        "proposal_ticket": _required_text(proposal_ticket, "proposal_ticket"),
        "rationale": _required_text(rationale, "rationale"),
        "proposed_at_utc": timestamp.isoformat(),
        "proposal_status": PROPOSAL_STATUS,
        "next_action": "named_dry_run_proposal_review_required",
    }
    document = {
        "proposal_id": "ipdp-" + _digest(core)[:24],
        "proposal_revision": 1,
        **core,
        **{field: False for field in SAFETY_FIELDS},
        "proposal_contract_version": PROPOSAL_CONTRACT_VERSION,
    }
    document["proposal_sha256"] = _digest(document)
    verify_dry_run_implementation_proposal(
        document,
        disposition_document,
        revision_sensitivity_summary,
        manifest_document,
        package_document,
        source_decision,
        source_sensitivity_summary,
        repository_root=repository_root,
    )
    return document


def verify_dry_run_implementation_proposal(
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_sensitivity_summary: pd.DataFrame,
    revision_sensitivity_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_root: Path,
) -> None:
    """Verify proposal bindings, Git base, policy diff, and no-application evidence."""
    document = dict(proposal)
    if document.get("proposal_contract_version") != PROPOSAL_CONTRACT_VERSION:
        raise IntervalPolicyDryRunProposalError("Unsupported proposal contract.")
    proposal_id = _required_text(document.get("proposal_id"), "proposal_id")
    if not PROPOSAL_ID_PATTERN.fullmatch(proposal_id):
        raise IntervalPolicyDryRunProposalError("proposal_id is malformed.")
    if document.get("proposal_revision") != 1:
        raise IntervalPolicyDryRunProposalError("proposal_revision must be 1.")
    expected_hash = _digest(
        {key: value for key, value in document.items() if key != "proposal_sha256"}
    )
    if document.get("proposal_sha256") != expected_hash:
        raise IntervalPolicyDryRunProposalError("Proposal hash is invalid.")
    if document.get("proposal_status") != PROPOSAL_STATUS:
        raise IntervalPolicyDryRunProposalError("Proposal status is invalid.")
    if document.get("next_action") != "named_dry_run_proposal_review_required":
        raise IntervalPolicyDryRunProposalError("Proposal next action is invalid.")

    values = {
        "disposition": dict(disposition),
        "summary": revision_sensitivity_summary,
        "manifest": dict(revision_sensitivity_manifest),
        "package": dict(revision_package),
        "decision": dict(source_decision),
        "sensitivity_summary": source_sensitivity_summary,
    }
    _call_verifier(verify_revision_disposition, values)
    _call_verifier(verify_candidate_revision_package, values)

    current = inspect_repository_base(
        repository_root,
        active_policy_source_path=document["active_policy_source_path"],
    )
    for key, value in current.items():
        if document.get(key) != value:
            raise IntervalPolicyDryRunProposalError(
                f"Repository base binding {key} is stale or inconsistent."
            )
    revised = dict(revision_package["revised_candidate"])
    proposed_snapshot = _policy_snapshot(revised)
    active_snapshot = asdict(PredictionIntervalMonitoringConfig())
    if _canonical(document.get("active_policy_snapshot")) != _canonical(
        active_snapshot
    ):
        raise IntervalPolicyDryRunProposalError(
            "Active policy snapshot is inconsistent."
        )
    if _canonical(document.get("proposed_policy_snapshot")) != _canonical(
        proposed_snapshot
    ):
        raise IntervalPolicyDryRunProposalError(
            "Proposed policy snapshot is inconsistent."
        )
    if document.get("active_policy_sha256") != _digest(active_snapshot):
        raise IntervalPolicyDryRunProposalError("Active policy digest is invalid.")
    if document.get("proposed_policy_sha256") != _digest(proposed_snapshot):
        raise IntervalPolicyDryRunProposalError("Proposed policy digest is invalid.")

    expected_changes = [
        {
            "field": name,
            "active_value": active_snapshot[name],
            "proposed_value": proposed_snapshot[name],
            "target_paths": list(document["target_paths"]),
            "application_state": "not_applied",
        }
        for name in _policy_fields()
        if active_snapshot[name] != proposed_snapshot[name]
    ]
    if _canonical(document.get("changed_thresholds")) != _canonical(
        expected_changes
    ):
        raise IntervalPolicyDryRunProposalError(
            "Changed-threshold evidence is inconsistent."
        )
    if document.get("changed_threshold_count") != len(expected_changes):
        raise IntervalPolicyDryRunProposalError(
            "changed_threshold_count is inconsistent."
        )
    for field in SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyDryRunProposalError(
                f"Proposal safety field {field} must be false."
            )


def render_dry_run_implementation_proposal(proposal: Mapping[str, Any]) -> str:
    document = dict(proposal)
    lines = [
        "# Interval-policy dry-run implementation proposal",
        "",
        f"- Proposal: `{document['proposal_id']}`",
        f"- Repository base: `{document['repository_base_commit_sha']}`",
        f"- Revised candidate: `{document['revised_candidate_id']}`",
        f"- Changed thresholds: {document['changed_threshold_count']}",
        f"- Status: `{document['proposal_status']}`",
        "",
        "| Field | Active | Proposed |",
        "| --- | ---: | ---: |",
    ]
    for item in document["changed_thresholds"]:
        lines.append(
            f"| `{item['field']}` | `{item['active_value']}` | "
            f"`{item['proposed_value']}` |"
        )
    lines.extend(
        [
            "",
            "This proposal is a repository-base-bound dry run only. It does not "
            "apply a patch, update the active policy, activate thresholds, "
            "recalibrate intervals, change models or schedules, deliver alerts, "
            "deploy, or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def write_dry_run_implementation_proposal(
    output_directory: Path,
    proposal: Mapping[str, Any],
) -> tuple[Path, Path]:
    """Write immutable JSON and Markdown proposal evidence."""
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    proposal_id = proposal["proposal_id"]
    json_path = output_directory / (
        f"interval_policy_dry_run_proposal_{proposal_id}.json"
    )
    markdown_path = output_directory / (
        f"interval_policy_dry_run_proposal_{proposal_id}.md"
    )
    for path in (json_path, markdown_path):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    temporary_json = json_path.with_name(f".{json_path.name}.tmp")
    temporary_markdown = markdown_path.with_name(f".{markdown_path.name}.tmp")
    try:
        temporary_json.write_text(
            json.dumps(_canonical(proposal), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_markdown.write_text(
            render_dry_run_implementation_proposal(proposal), encoding="utf-8"
        )
        temporary_json.replace(json_path)
        temporary_markdown.replace(markdown_path)
    finally:
        temporary_json.unlink(missing_ok=True)
        temporary_markdown.unlink(missing_ok=True)
    return json_path, markdown_path


__all__ = [
    "DEFAULT_TARGET_PATHS",
    "DEFAULT_VALIDATION_COMMANDS",
    "PROPOSAL_CONTRACT_VERSION",
    "PROPOSAL_STATUS",
    "SAFETY_FIELDS",
    "IntervalPolicyDryRunProposalError",
    "create_dry_run_implementation_proposal",
    "inspect_repository_base",
    "render_dry_run_implementation_proposal",
    "verify_dry_run_implementation_proposal",
    "write_dry_run_implementation_proposal",
]
