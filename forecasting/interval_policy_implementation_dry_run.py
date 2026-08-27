from __future__ import annotations

import ast
import difflib
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from forecasting._interval_policy_candidate_revision_common import (
    CONFIG_FIELDS,
    THRESHOLD_FIELDS,
    active_policy_snapshot,
    canonical,
    digest,
    prepare_candidate_snapshot,
    required_text,
    utc_timestamp,
)
from forecasting.interval_policy_candidate_revision import (
    verify_candidate_revision_package,
)
from forecasting.interval_policy_revision_disposition import (
    verify_revision_sensitivity_disposition,
)


IMPLEMENTATION_DRY_RUN_CONTRACT_VERSION = (
    "interval-policy-implementation-dry-run-v1"
)
IMPLEMENTATION_DRY_RUN_ID_PATTERN = re.compile(r"^ipid-[0-9a-f]{24}$")
POLICY_SOURCE_PATH = "forecasting/interval_monitoring.py"
POLICY_CLASS_NAME = "PredictionIntervalMonitoringConfig"
IMPLEMENTATION_DRY_RUN_SAFETY_FIELDS = (
    "implementation_authorized",
    "implementation_applied",
    "code_change_created",
    "source_file_mutated",
    "threshold_activation_authorized",
    "active_policy_updated",
    "candidate_thresholds_activated",
    "source_disposition_mutated",
    "source_package_mutated",
    "source_decision_mutated",
    "source_sensitivity_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)


class IntervalPolicyImplementationDryRunError(ValueError):
    """Raised when a base-bound implementation proposal is unsafe."""


def _full_sha(value: Any, name: str) -> str:
    text = required_text(value, name)
    if not re.fullmatch(r"[0-9a-f]{40}", text):
        raise IntervalPolicyImplementationDryRunError(
            f"{name} must be a lowercase 40-character Git SHA."
        )
    return text


def _unique_texts(
    values: Iterable[Any],
    name: str,
    *,
    required: bool,
    minimum_length: int = 1,
) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise IntervalPolicyImplementationDryRunError(
            f"{name} must be a list of strings."
        )
    result = [required_text(item, name) for item in values]
    if required and not result:
        raise IntervalPolicyImplementationDryRunError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyImplementationDryRunError(
            f"{name} must not contain duplicates."
        )
    if any(len(item) < minimum_length for item in result):
        raise IntervalPolicyImplementationDryRunError(
            f"Each {name} item must contain at least {minimum_length} characters."
        )
    return result


def source_sha256(source_text: str) -> str:
    return hashlib.sha256(source_text.encode("utf-8")).hexdigest()


def source_git_blob_sha1(source_text: str) -> str:
    payload = source_text.encode("utf-8")
    header = f"blob {len(payload)}\0".encode("utf-8")
    return hashlib.sha1(header + payload).hexdigest()


def _literal_defaults(source_text: str) -> dict[str, Any]:
    try:
        tree = ast.parse(source_text)
    except SyntaxError as exc:
        raise IntervalPolicyImplementationDryRunError(
            "Policy source must contain valid Python."
        ) from exc
    target = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == POLICY_CLASS_NAME
        ),
        None,
    )
    if target is None:
        raise IntervalPolicyImplementationDryRunError(
            f"Policy source is missing {POLICY_CLASS_NAME}."
        )
    defaults: dict[str, Any] = {}
    for statement in target.body:
        name = None
        value = None
        if isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name
        ):
            name = statement.target.id
            value = statement.value
        elif (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
        ):
            name = statement.targets[0].id
            value = statement.value
        if name not in CONFIG_FIELDS or value is None:
            continue
        if name == "policy_version":
            if not isinstance(value, ast.Name) or value.id != "POLICY_VERSION":
                raise IntervalPolicyImplementationDryRunError(
                    "policy_version must continue to reference POLICY_VERSION."
                )
            defaults[name] = active_policy_snapshot()["policy_version"]
            continue
        try:
            defaults[name] = ast.literal_eval(value)
        except (ValueError, TypeError) as exc:
            raise IntervalPolicyImplementationDryRunError(
                f"{name} must use a literal default in the policy source."
            ) from exc
    missing = sorted(set(CONFIG_FIELDS) - set(defaults))
    if missing:
        raise IntervalPolicyImplementationDryRunError(
            "Policy source is missing defaults for: " + ", ".join(missing) + "."
        )
    return canonical(defaults)


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(float(value))
    return repr(value)


def _build_patch(
    source_text: str,
    current: Mapping[str, Any],
    proposed: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    lines = source_text.splitlines(keepends=True)
    revised_lines = list(lines)
    edits: list[dict[str, Any]] = []
    for field in THRESHOLD_FIELDS:
        if canonical(current[field]) == canonical(proposed[field]):
            continue
        pattern = re.compile(
            rf"^(?P<indent>\s*)(?P<prefix>{re.escape(field)}\s*:\s*[^=]+?=\s*)"
            rf"(?P<value>[^#\n]+?)(?P<suffix>\s*(?:#.*)?)(?P<newline>\n?)$"
        )
        matches = [
            (index, pattern.match(line))
            for index, line in enumerate(lines)
            if pattern.match(line)
        ]
        if len(matches) != 1:
            raise IntervalPolicyImplementationDryRunError(
                f"Expected exactly one source default for {field}."
            )
        index, match = matches[0]
        assert match is not None
        replacement = (
            f"{match.group('indent')}{match.group('prefix')}"
            f"{_format_value(proposed[field])}{match.group('suffix')}"
            f"{match.group('newline')}"
        )
        revised_lines[index] = replacement
        edits.append(
            {
                "field": field,
                "line_number": index + 1,
                "current_value": current[field],
                "proposed_value": proposed[field],
                "before_line": lines[index].rstrip("\n"),
                "after_line": replacement.rstrip("\n"),
            }
        )
    patch = "".join(
        difflib.unified_diff(
            lines,
            revised_lines,
            fromfile=f"a/{POLICY_SOURCE_PATH}",
            tofile=f"b/{POLICY_SOURCE_PATH}",
        )
    )
    if not edits or not patch:
        raise IntervalPolicyImplementationDryRunError(
            "The dry run must contain at least one source edit."
        )
    return edits, patch


def create_interval_policy_implementation_dry_run(
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_base_commit: str,
    repository_base_tree: str,
    policy_source_text: str,
    prepared_by: str,
    preparer_role: str,
    implementation_ticket: str,
    rationale: str,
    intended_paths: Iterable[Any],
    validation_commands: Iterable[Any],
    prepared_at_utc: Any | None = None,
) -> dict[str, Any]:
    """Create one immutable dry-run proposal without changing repository code."""
    disposition_document = dict(disposition)
    package_document = dict(revision_package)
    decision_document = dict(source_decision)
    verify_revision_sensitivity_disposition(
        disposition_document, revision_summary, revision_manifest
    )
    verify_candidate_revision_package(
        package_document, decision_document, source_sensitivity_summary
    )
    if disposition_document.get("disposition") != (
        "suitable_for_separate_implementation_proposal"
    ):
        raise IntervalPolicyImplementationDryRunError(
            "G33 requires a suitable_for_separate_implementation_proposal disposition."
        )
    if disposition_document.get("follow_up_human_action_required") is not True:
        raise IntervalPolicyImplementationDryRunError(
            "The G32 disposition must require separate human follow-up."
        )
    revised = prepare_candidate_snapshot(
        package_document.get("revised_candidate", {}),
        label="revised_candidate",
    )
    if disposition_document.get("target_candidate_id") != revised["candidate_id"]:
        raise IntervalPolicyImplementationDryRunError(
            "Disposition target does not match the revised candidate."
        )
    if disposition_document.get("revised_candidate_sha256") != (
        package_document.get("revised_candidate_sha256")
    ):
        raise IntervalPolicyImplementationDryRunError(
            "Disposition and package revised-candidate digests differ."
        )
    base_commit = _full_sha(repository_base_commit, "repository_base_commit")
    base_tree = _full_sha(repository_base_tree, "repository_base_tree")
    if not isinstance(policy_source_text, str) or not policy_source_text.strip():
        raise IntervalPolicyImplementationDryRunError(
            "policy_source_text must be non-empty text."
        )
    source_text = policy_source_text
    current = active_policy_snapshot()
    parsed = _literal_defaults(source_text)
    if canonical(parsed) != canonical(current):
        raise IntervalPolicyImplementationDryRunError(
            "Policy source defaults do not match the checked-in active policy."
        )
    if canonical(package_document.get("active_policy_snapshot")) != canonical(
        current
    ):
        raise IntervalPolicyImplementationDryRunError(
            "Revision package active-policy snapshot is stale."
        )
    proposed = canonical({field: revised[field] for field in CONFIG_FIELDS})
    if proposed["policy_version"] != current["policy_version"]:
        raise IntervalPolicyImplementationDryRunError(
            "The proposal cannot change policy_version."
        )
    changes = [
        {
            "field": field,
            "current_value": current[field],
            "proposed_value": proposed[field],
        }
        for field in THRESHOLD_FIELDS
        if canonical(current[field]) != canonical(proposed[field])
    ]
    if not changes:
        raise IntervalPolicyImplementationDryRunError(
            "The revised candidate does not change the active policy."
        )
    source_edits, patch = _build_patch(source_text, current, proposed)
    if [item["field"] for item in source_edits] != [
        item["field"] for item in changes
    ]:
        raise IntervalPolicyImplementationDryRunError(
            "Source edits do not cover every active-policy change."
        )
    paths = _unique_texts(
        intended_paths, "intended_paths", required=True, minimum_length=3
    )
    if POLICY_SOURCE_PATH not in paths:
        raise IntervalPolicyImplementationDryRunError(
            f"intended_paths must include {POLICY_SOURCE_PATH}."
        )
    commands = _unique_texts(
        validation_commands,
        "validation_commands",
        required=True,
        minimum_length=8,
    )
    if not any("compileall" in command for command in commands):
        raise IntervalPolicyImplementationDryRunError(
            "validation_commands must include Python compilation."
        )
    if not any("pytest" in command for command in commands):
        raise IntervalPolicyImplementationDryRunError(
            "validation_commands must include pytest."
        )
    prepared_at = utc_timestamp(
        prepared_at_utc or datetime.now(timezone.utc), "prepared_at_utc"
    )
    disposed_at = utc_timestamp(
        disposition_document.get("disposed_at_utc"), "disposed_at_utc"
    )
    if prepared_at < disposed_at:
        raise IntervalPolicyImplementationDryRunError(
            "The dry-run proposal cannot predate the G32 disposition."
        )
    core = {
        "source_disposition_id": disposition_document["disposition_id"],
        "source_disposition_sha256": disposition_document[
            "disposition_sha256"
        ],
        "source_revision_sensitivity_run_id": disposition_document[
            "sensitivity_run_id"
        ],
        "source_revision_sensitivity_summary_sha256": disposition_document[
            "revision_sensitivity_summary_sha256"
        ],
        "source_revision_sensitivity_manifest_sha256": disposition_document[
            "revision_sensitivity_manifest_sha256"
        ],
        "source_revision_package_id": package_document["revision_package_id"],
        "source_revision_package_sha256": package_document[
            "revision_package_sha256"
        ],
        "source_decision_id": decision_document["decision_id"],
        "source_decision_sha256": decision_document["decision_sha256"],
        "revised_candidate_id": revised["candidate_id"],
        "revised_candidate_version": revised["candidate_version"],
        "revised_candidate_sha256": package_document[
            "revised_candidate_sha256"
        ],
        "repository_base_commit": base_commit,
        "repository_base_tree": base_tree,
        "policy_source_path": POLICY_SOURCE_PATH,
        "policy_source_sha256": source_sha256(source_text),
        "policy_source_git_blob_sha1": source_git_blob_sha1(source_text),
        "active_policy_snapshot": current,
        "active_policy_sha256": digest(current),
        "proposed_policy_snapshot": proposed,
        "proposed_policy_sha256": digest(proposed),
        "active_to_proposed_changes": changes,
        "source_edits": source_edits,
        "dry_run_patch": patch,
        "intended_paths": paths,
        "validation_commands": commands,
        "prepared_by": required_text(prepared_by, "prepared_by"),
        "preparer_role": required_text(preparer_role, "preparer_role"),
        "implementation_ticket": required_text(
            implementation_ticket, "implementation_ticket"
        ),
        "rationale": required_text(rationale, "rationale", minimum_length=30),
        "prepared_at_utc": prepared_at.isoformat(),
        "compatibility_status": "ready_for_separate_code_change_review",
        "next_review_action": "named_implementation_dry_run_review_required",
        "repository_base_must_match": True,
    }
    document = {
        "implementation_dry_run_id": "ipid-" + digest(core)[:24],
        "implementation_dry_run_revision": 1,
        **core,
        "human_preparation_confirmed": True,
        **{
            field: False
            for field in IMPLEMENTATION_DRY_RUN_SAFETY_FIELDS
        },
        "implementation_dry_run_contract_version": (
            IMPLEMENTATION_DRY_RUN_CONTRACT_VERSION
        ),
    }
    document["implementation_dry_run_sha256"] = digest(document)
    verify_interval_policy_implementation_dry_run(
        document,
        disposition_document,
        revision_summary,
        revision_manifest,
        package_document,
        decision_document,
        source_sensitivity_summary,
        repository_base_commit=base_commit,
        repository_base_tree=base_tree,
        policy_source_text=source_text,
    )
    return document


def verify_interval_policy_implementation_dry_run(
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_base_commit: str,
    repository_base_tree: str,
    policy_source_text: str,
) -> None:
    """Verify proposal integrity against the current repository base."""
    document = dict(proposal)
    disposition_document = dict(disposition)
    package_document = dict(revision_package)
    decision_document = dict(source_decision)
    verify_revision_sensitivity_disposition(
        disposition_document, revision_summary, revision_manifest
    )
    verify_candidate_revision_package(
        package_document, decision_document, source_sensitivity_summary
    )
    if disposition_document.get("disposition") != (
        "suitable_for_separate_implementation_proposal"
    ):
        raise IntervalPolicyImplementationDryRunError(
            "The source disposition is not suitable for implementation planning."
        )
    if document.get("implementation_dry_run_contract_version") != (
        IMPLEMENTATION_DRY_RUN_CONTRACT_VERSION
    ):
        raise IntervalPolicyImplementationDryRunError(
            "Implementation dry-run contract version is invalid."
        )
    proposal_id = required_text(
        document.get("implementation_dry_run_id"),
        "implementation_dry_run_id",
    )
    if not IMPLEMENTATION_DRY_RUN_ID_PATTERN.fullmatch(proposal_id):
        raise IntervalPolicyImplementationDryRunError(
            "implementation_dry_run_id is malformed."
        )
    if document.get("implementation_dry_run_revision") != 1:
        raise IntervalPolicyImplementationDryRunError(
            "implementation_dry_run_revision must be 1."
        )
    expected_hash = digest(
        {
            key: value
            for key, value in document.items()
            if key != "implementation_dry_run_sha256"
        }
    )
    if document.get("implementation_dry_run_sha256") != expected_hash:
        raise IntervalPolicyImplementationDryRunError(
            "Implementation dry-run hash is invalid."
        )
    bindings = {
        "source_disposition_id": disposition_document["disposition_id"],
        "source_disposition_sha256": disposition_document[
            "disposition_sha256"
        ],
        "source_revision_sensitivity_run_id": disposition_document[
            "sensitivity_run_id"
        ],
        "source_revision_sensitivity_summary_sha256": disposition_document[
            "revision_sensitivity_summary_sha256"
        ],
        "source_revision_sensitivity_manifest_sha256": disposition_document[
            "revision_sensitivity_manifest_sha256"
        ],
        "source_revision_package_id": package_document["revision_package_id"],
        "source_revision_package_sha256": package_document[
            "revision_package_sha256"
        ],
        "source_decision_id": decision_document["decision_id"],
        "source_decision_sha256": decision_document["decision_sha256"],
        "revised_candidate_id": package_document["revised_candidate"][
            "candidate_id"
        ],
        "revised_candidate_version": package_document["revised_candidate"][
            "candidate_version"
        ],
        "revised_candidate_sha256": package_document[
            "revised_candidate_sha256"
        ],
    }
    for field, expected in bindings.items():
        if document.get(field) != expected:
            raise IntervalPolicyImplementationDryRunError(
                f"Implementation dry-run {field} is inconsistent."
            )
    base_commit = _full_sha(repository_base_commit, "repository_base_commit")
    base_tree = _full_sha(repository_base_tree, "repository_base_tree")
    if document.get("repository_base_commit") != base_commit:
        raise IntervalPolicyImplementationDryRunError(
            "Repository base commit has changed."
        )
    if document.get("repository_base_tree") != base_tree:
        raise IntervalPolicyImplementationDryRunError(
            "Repository base tree has changed."
        )
    if not isinstance(policy_source_text, str) or not policy_source_text.strip():
        raise IntervalPolicyImplementationDryRunError(
            "policy_source_text must be non-empty text."
        )
    source_text = policy_source_text
    if document.get("policy_source_path") != POLICY_SOURCE_PATH:
        raise IntervalPolicyImplementationDryRunError(
            "Policy source path is invalid."
        )
    if document.get("policy_source_sha256") != source_sha256(source_text):
        raise IntervalPolicyImplementationDryRunError(
            "Policy source SHA-256 has changed."
        )
    if document.get("policy_source_git_blob_sha1") != source_git_blob_sha1(
        source_text
    ):
        raise IntervalPolicyImplementationDryRunError(
            "Policy source Git blob identity has changed."
        )
    current = active_policy_snapshot()
    if canonical(_literal_defaults(source_text)) != canonical(current):
        raise IntervalPolicyImplementationDryRunError(
            "Policy source defaults no longer match the active policy."
        )
    if canonical(document.get("active_policy_snapshot")) != canonical(current):
        raise IntervalPolicyImplementationDryRunError(
            "Active-policy snapshot is inconsistent."
        )
    if document.get("active_policy_sha256") != digest(current):
        raise IntervalPolicyImplementationDryRunError(
            "Active-policy digest is invalid."
        )
    revised = prepare_candidate_snapshot(
        package_document["revised_candidate"], label="revised_candidate"
    )
    proposed = canonical({field: revised[field] for field in CONFIG_FIELDS})
    if canonical(document.get("proposed_policy_snapshot")) != proposed:
        raise IntervalPolicyImplementationDryRunError(
            "Proposed-policy snapshot is inconsistent."
        )
    if document.get("proposed_policy_sha256") != digest(proposed):
        raise IntervalPolicyImplementationDryRunError(
            "Proposed-policy digest is invalid."
        )
    expected_changes = [
        {
            "field": field,
            "current_value": current[field],
            "proposed_value": proposed[field],
        }
        for field in THRESHOLD_FIELDS
        if canonical(current[field]) != canonical(proposed[field])
    ]
    expected_edits, expected_patch = _build_patch(source_text, current, proposed)
    if canonical(document.get("active_to_proposed_changes")) != canonical(
        expected_changes
    ):
        raise IntervalPolicyImplementationDryRunError(
            "Active-to-proposed changes are inconsistent."
        )
    if canonical(document.get("source_edits")) != canonical(expected_edits):
        raise IntervalPolicyImplementationDryRunError(
            "Source edits are inconsistent."
        )
    if document.get("dry_run_patch") != expected_patch:
        raise IntervalPolicyImplementationDryRunError(
            "Dry-run patch is inconsistent."
        )
    paths = _unique_texts(
        document.get("intended_paths", ()),
        "intended_paths",
        required=True,
        minimum_length=3,
    )
    if POLICY_SOURCE_PATH not in paths:
        raise IntervalPolicyImplementationDryRunError(
            "Dry-run intended paths omit the policy source."
        )
    commands = _unique_texts(
        document.get("validation_commands", ()),
        "validation_commands",
        required=True,
        minimum_length=8,
    )
    if not any("compileall" in command for command in commands) or not any(
        "pytest" in command for command in commands
    ):
        raise IntervalPolicyImplementationDryRunError(
            "Dry-run validation commands are incomplete."
        )
    required_text(document.get("prepared_by"), "prepared_by")
    required_text(document.get("preparer_role"), "preparer_role")
    required_text(document.get("implementation_ticket"), "implementation_ticket")
    required_text(document.get("rationale"), "rationale", minimum_length=30)
    prepared_at = utc_timestamp(
        document.get("prepared_at_utc"), "prepared_at_utc"
    )
    if prepared_at < utc_timestamp(
        disposition_document.get("disposed_at_utc"), "disposed_at_utc"
    ):
        raise IntervalPolicyImplementationDryRunError(
            "Dry-run proposal predates its disposition."
        )
    if document.get("compatibility_status") != (
        "ready_for_separate_code_change_review"
    ):
        raise IntervalPolicyImplementationDryRunError(
            "compatibility_status is invalid."
        )
    if document.get("next_review_action") != (
        "named_implementation_dry_run_review_required"
    ):
        raise IntervalPolicyImplementationDryRunError(
            "next_review_action is invalid."
        )
    if document.get("repository_base_must_match") is not True:
        raise IntervalPolicyImplementationDryRunError(
            "Repository-base matching must be required."
        )
    if document.get("human_preparation_confirmed") is not True:
        raise IntervalPolicyImplementationDryRunError(
            "Human preparation must be confirmed."
        )
    for field in IMPLEMENTATION_DRY_RUN_SAFETY_FIELDS:
        if document.get(field) is not False:
            raise IntervalPolicyImplementationDryRunError(
                f"Implementation dry-run safety field {field} must be false."
            )


def render_interval_policy_implementation_dry_run(
    proposal: Mapping[str, Any],
) -> str:
    document = dict(proposal)
    lines = [
        "# Interval-policy implementation dry run",
        "",
        f"- Proposal ID: `{document['implementation_dry_run_id']}`",
        f"- Repository base: `{document['repository_base_commit']}`",
        f"- Repository tree: `{document['repository_base_tree']}`",
        f"- Revised candidate: `{document['revised_candidate_id']}`",
        f"- Prepared by: {document['prepared_by']} ({document['preparer_role']})",
        f"- Ticket: `{document['implementation_ticket']}`",
        f"- Prepared at: `{document['prepared_at_utc']}`",
        "",
        "## Rationale",
        "",
        document["rationale"],
        "",
        "## Proposed source changes",
        "",
        "| Field | Current | Proposed | Line |",
        "| --- | ---: | ---: | ---: |",
    ]
    for item in document["source_edits"]:
        lines.append(
            f"| {item['field']} | {item['current_value']} | "
            f"{item['proposed_value']} | {item['line_number']} |"
        )
    lines.extend(
        [
            "",
            "## Dry-run patch",
            "",
            "```diff",
            document["dry_run_patch"].rstrip(),
            "```",
            "",
            "## Validation commands",
            "",
        ]
    )
    lines.extend(f"- `{command}`" for command in document["validation_commands"])
    lines.extend(
        [
            "",
            "This is repository-base-bound planning evidence only.",
            "It does not create or apply a code change, update or activate the "
            "monitoring policy, recalibrate intervals, change models or schedules, "
            "deliver alerts, deploy, or publish externally.",
            "",
        ]
    )
    return "\n".join(lines)


def write_interval_policy_implementation_dry_run(
    output_directory: Path,
    proposal: Mapping[str, Any],
    disposition: Mapping[str, Any],
    revision_summary: pd.DataFrame,
    revision_manifest: Mapping[str, Any],
    revision_package: Mapping[str, Any],
    source_decision: Mapping[str, Any],
    source_sensitivity_summary: pd.DataFrame,
    *,
    repository_base_commit: str,
    repository_base_tree: str,
    policy_source_text: str,
) -> tuple[Path, Path]:
    document = dict(proposal)
    verify_interval_policy_implementation_dry_run(
        document,
        disposition,
        revision_summary,
        revision_manifest,
        revision_package,
        source_decision,
        source_sensitivity_summary,
        repository_base_commit=repository_base_commit,
        repository_base_tree=repository_base_tree,
        policy_source_text=policy_source_text,
    )
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    proposal_id = document["implementation_dry_run_id"]
    json_path = output_directory / (
        f"interval_policy_implementation_dry_run_{proposal_id}.json"
    )
    markdown_path = output_directory / (
        f"interval_policy_implementation_dry_run_{proposal_id}.md"
    )
    temporary_json = json_path.with_name(f".{json_path.name}.tmp")
    temporary_markdown = markdown_path.with_name(f".{markdown_path.name}.tmp")
    for path in (
        json_path,
        markdown_path,
        temporary_json,
        temporary_markdown,
    ):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite {path}.")
    try:
        temporary_json.write_text(
            json.dumps(canonical(document), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_markdown.write_text(
            render_interval_policy_implementation_dry_run(document),
            encoding="utf-8",
        )
        temporary_json.replace(json_path)
        temporary_markdown.replace(markdown_path)
    finally:
        temporary_json.unlink(missing_ok=True)
        temporary_markdown.unlink(missing_ok=True)
    return json_path, markdown_path


def read_frame(path: Path) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise IntervalPolicyImplementationDryRunError(
        "Tabular evidence must be CSV or Parquet."
    )


def read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntervalPolicyImplementationDryRunError(
            f"{label} must be readable JSON."
        ) from exc
    if not isinstance(value, dict):
        raise IntervalPolicyImplementationDryRunError(
            f"{label} must be a JSON object."
        )
    return value


__all__ = [
    "IMPLEMENTATION_DRY_RUN_CONTRACT_VERSION",
    "IMPLEMENTATION_DRY_RUN_SAFETY_FIELDS",
    "IntervalPolicyImplementationDryRunError",
    "create_interval_policy_implementation_dry_run",
    "read_frame",
    "read_json",
    "render_interval_policy_implementation_dry_run",
    "source_git_blob_sha1",
    "source_sha256",
    "verify_interval_policy_implementation_dry_run",
    "write_interval_policy_implementation_dry_run",
]
