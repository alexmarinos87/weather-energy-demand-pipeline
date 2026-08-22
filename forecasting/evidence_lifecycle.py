from __future__ import annotations

import fnmatch
import hashlib
import json
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path
from typing import Any

import pandas as pd

from forecasting.model_registry import load_candidate_history


POLICY_CONTRACT_VERSION = "evidence-retention-policy-v1"
INVENTORY_CONTRACT_VERSION = "evidence-inventory-v1"
PLAN_CONTRACT_VERSION = "evidence-lifecycle-plan-v1"
SUMMARY_CONTRACT_VERSION = "evidence-lifecycle-summary-v1"
PLANNED_ACTIONS = {"retain", "compact_candidate", "quarantine_candidate"}
CANDIDATE_STATES = {"draft", "review_requested", "approved", "rejected", "retired"}


class EvidenceLifecycleError(ValueError):
    """Raised when evidence inventory or lifecycle planning is unsafe."""


def _utc(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceLifecycleError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise EvidenceLifecycleError(f"{name} must be timezone-aware.")
    return timestamp.tz_convert("UTC")


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise EvidenceLifecycleError(f"{name} must be a non-negative integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceLifecycleError(
            f"{name} must be a non-negative integer."
        ) from exc
    if parsed < 0:
        raise EvidenceLifecycleError(f"{name} must be a non-negative integer.")
    return parsed


def _optional_positive_int(value: Any, name: str) -> int | None:
    if value is None:
        return None
    parsed = _non_negative_int(value, name)
    if parsed < 1:
        raise EvidenceLifecycleError(f"{name} must be null or at least 1.")
    return parsed


def _safe_relative(value: Any, name: str) -> str:
    text = str(value).strip().replace("\\", "/")
    if not text or text.startswith("/") or ".." in Path(text).parts:
        raise EvidenceLifecycleError(f"{name} must be a safe relative path pattern.")
    return text


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_retention_policy(path: Path) -> dict[str, Any]:
    """Load and validate the versioned dry-run retention policy."""
    raw = Path(path).read_bytes()
    policy = json.loads(raw.decode("utf-8"))
    if policy.get("contract_version") != POLICY_CONTRACT_VERSION:
        raise EvidenceLifecycleError(
            f"Policy contract must be {POLICY_CONTRACT_VERSION}."
        )
    if policy.get("unclassified_action") != "retain":
        raise EvidenceLifecycleError("Unclassified evidence must be retained by default.")
    states = policy.get("protected_candidate_states")
    if not isinstance(states, list) or not states:
        raise EvidenceLifecycleError(
            "protected_candidate_states must be a non-empty list."
        )
    unsupported_states = sorted(set(states) - CANDIDATE_STATES)
    if unsupported_states:
        raise EvidenceLifecycleError(
            "Policy contains unsupported candidate states: "
            + ", ".join(unsupported_states)
            + "."
        )
    excluded = policy.get("excluded_prefixes", [])
    if not isinstance(excluded, list):
        raise EvidenceLifecycleError("excluded_prefixes must be a list.")
    policy["excluded_prefixes"] = [
        _safe_relative(value, "excluded_prefix") for value in excluded
    ]
    categories = policy.get("categories")
    if not isinstance(categories, list) or not categories:
        raise EvidenceLifecycleError("Policy must define at least one category.")
    normalized: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw_rule in enumerate(categories):
        if not isinstance(raw_rule, dict):
            raise EvidenceLifecycleError(f"Policy category {index} must be an object.")
        name = str(raw_rule.get("category", "")).strip()
        if not name or name in names:
            raise EvidenceLifecycleError(
                "Every policy category must have a unique non-empty name."
            )
        names.add(name)
        patterns = raw_rule.get("patterns")
        if not isinstance(patterns, list) or not patterns:
            raise EvidenceLifecycleError(
                f"Category {name} must define at least one pattern."
            )
        patterns = [_safe_relative(value, f"{name}.pattern") for value in patterns]
        retention = _optional_positive_int(
            raw_rule.get("retention_days"), f"{name}.retention_days"
        )
        compact_after = _optional_positive_int(
            raw_rule.get("compact_after_days"), f"{name}.compact_after_days"
        )
        if retention is not None and compact_after is not None and compact_after >= retention:
            raise EvidenceLifecycleError(
                f"{name}.compact_after_days must be below retention_days."
            )
        formats = sorted(
            {
                str(value).strip().lower()
                for value in raw_rule.get("compaction_formats", [])
                if str(value).strip()
            }
        )
        if any(not suffix.startswith(".") for suffix in formats):
            raise EvidenceLifecycleError(
                f"{name}.compaction_formats must use suffixes such as .parquet."
            )
        minimum_files = _non_negative_int(
            raw_rule.get("min_compaction_files", 0),
            f"{name}.min_compaction_files",
        )
        byte_bound = _non_negative_int(
            raw_rule.get("max_compaction_source_bytes", 0),
            f"{name}.max_compaction_source_bytes",
        )
        if compact_after is None:
            if formats or minimum_files or byte_bound:
                raise EvidenceLifecycleError(
                    f"{name} cannot configure compaction without compact_after_days."
                )
        elif not formats or minimum_files < 2 or byte_bound < 1:
            raise EvidenceLifecycleError(
                f"{name} compaction requires formats, at least two files, and a byte bound."
            )
        normalized.append(
            {
                "category": name,
                "patterns": patterns,
                "retention_days": retention,
                "min_keep_latest": _non_negative_int(
                    raw_rule.get("min_keep_latest", 0),
                    f"{name}.min_keep_latest",
                ),
                "compact_after_days": compact_after,
                "compaction_formats": formats,
                "min_compaction_files": minimum_files,
                "max_compaction_source_bytes": byte_bound,
                "always_protect": bool(raw_rule.get("always_protect", False)),
            }
        )
    policy["categories"] = normalized
    policy["policy_sha256"] = hashlib.sha256(raw).hexdigest()
    return policy


def _category(relative_path: str, policy: dict[str, Any]) -> dict[str, Any] | None:
    for rule in policy["categories"]:
        if any(fnmatch.fnmatchcase(relative_path, pattern) for pattern in rule["patterns"]):
            return rule
    return None


def inventory_evidence(
    data_root: Path,
    policy: dict[str, Any],
    *,
    as_of_utc: Any | None = None,
) -> pd.DataFrame:
    """Build a content-hashed inventory without mutating evidence."""
    root = Path(data_root).resolve()
    if not root.exists() or not root.is_dir():
        raise EvidenceLifecycleError(f"Data root is not a directory: {root}.")
    as_of = _utc(as_of_utc or datetime.now(timezone.utc), "as_of_utc")
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise EvidenceLifecycleError(
                f"Evidence inventory refuses symbolic links: {path}."
            )
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if any(relative.startswith(prefix) for prefix in policy["excluded_prefixes"]):
            continue
        stat = path.stat()
        modified = pd.Timestamp(stat.st_mtime, unit="s", tz="UTC")
        if modified > as_of:
            raise EvidenceLifecycleError(
                f"Evidence file modification time is after as_of_utc: {relative}."
            )
        rule = _category(relative, policy)
        effective = rule or {
            "category": "unclassified",
            "retention_days": None,
            "min_keep_latest": 0,
            "compact_after_days": None,
            "compaction_formats": [],
            "min_compaction_files": 0,
            "max_compaction_source_bytes": 0,
            "always_protect": False,
        }
        rows.append(
            {
                "inventory_as_of_utc": as_of,
                "relative_path": relative,
                "parent_path": Path(relative).parent.as_posix(),
                "file_name": path.name,
                "suffix": path.suffix.lower(),
                "category": effective["category"],
                "classified": rule is not None,
                "size_bytes": int(stat.st_size),
                "modified_at_utc": modified,
                "age_days": float((as_of - modified).total_seconds() / 86400.0),
                "sha256": _sha256_file(path),
                "retention_days": effective["retention_days"],
                "min_keep_latest": int(effective["min_keep_latest"]),
                "compact_after_days": effective["compact_after_days"],
                "compaction_formats": tuple(effective["compaction_formats"]),
                "min_compaction_files": int(effective["min_compaction_files"]),
                "max_compaction_source_bytes": int(
                    effective["max_compaction_source_bytes"]
                ),
                "always_protect": bool(effective["always_protect"]),
                "inventory_contract_version": INVENTORY_CONTRACT_VERSION,
            }
        )
    if not rows:
        raise EvidenceLifecycleError("No evidence files were found below data_root.")
    return pd.DataFrame(rows).sort_values("relative_path").reset_index(drop=True)


def load_protected_candidate_references(
    data_root: Path, policy: dict[str, Any]
) -> dict[str, str]:
    """Extract identifiers from verified protected candidate histories."""
    root = Path(data_root) / "model-registry"
    if not root.exists():
        return {}
    protected_states = set(policy["protected_candidate_states"])
    references: dict[str, str] = {}
    for directory in sorted(path for path in root.iterdir() if path.is_dir()):
        _, _, latest = load_candidate_history(directory)
        if latest["candidate_state"] not in protected_states:
            continue
        reason = f"candidate={latest['candidate_id']} state={latest['candidate_state']}"
        for key in (
            "candidate_id",
            "promotion_assessment_id",
            "comparison_run_id",
            "reconciliation_run_id",
            "provider_health_monitor_run_id",
            "code_commit_sha",
            "code_tree_sha",
        ):
            identifier = str(latest.get(key, "")).strip()
            if identifier:
                references[identifier] = reason
    return references


def _protection(relative_path: str, references: dict[str, str]) -> str | None:
    for identifier in sorted(references, key=len, reverse=True):
        if identifier in relative_path:
            return references[identifier]
    return None


def plan_evidence_lifecycle(
    inventory: pd.DataFrame,
    policy: dict[str, Any],
    *,
    protected_references: dict[str, str] | None = None,
    monthly_storage_cost_per_gib: float = 0.0,
    plan_created_at_utc: Any | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Create a non-mutating lifecycle recommendation and cost summary."""
    if inventory.empty:
        raise EvidenceLifecycleError("Inventory must not be empty.")
    cost = float(monthly_storage_cost_per_gib)
    if not isfinite(cost) or cost < 0:
        raise EvidenceLifecycleError(
            "monthly_storage_cost_per_gib must be finite and non-negative."
        )
    created_at = _utc(
        plan_created_at_utc or datetime.now(timezone.utc),
        "plan_created_at_utc",
    )
    plan = inventory.copy()
    ranking = plan.sort_values(
        ["category", "modified_at_utc", "relative_path"],
        ascending=[True, False, False],
    )
    ranks = ranking.groupby("category", sort=False).cumcount().add(1)
    plan["latest_rank"] = ranks.reindex(plan.index).astype(int)
    references = protected_references or {}
    plan["candidate_protection_reason"] = plan["relative_path"].map(
        lambda value: _protection(value, references)
    )
    plan["protected_by_candidate"] = plan["candidate_protection_reason"].notna()
    compactable = (
        plan["compact_after_days"].notna()
        & (plan["age_days"] > plan["compact_after_days"].fillna(float("inf")))
        & plan.apply(
            lambda row: row["suffix"] in set(row["compaction_formats"]), axis=1
        )
        & (plan["size_bytes"] <= plan["max_compaction_source_bytes"])
    )
    compact_counts = (
        plan.loc[compactable]
        .groupby(["category", "parent_path"], dropna=False)
        .size()
        .to_dict()
    )
    actions: list[str] = []
    reasons: list[str] = []
    for index, row in plan.iterrows():
        if row["always_protect"]:
            action, reason = "retain", "category is configured as always protected"
        elif row["protected_by_candidate"]:
            action, reason = "retain", row["candidate_protection_reason"]
        elif row["latest_rank"] <= row["min_keep_latest"]:
            action, reason = (
                "retain",
                f"within latest {row['min_keep_latest']} files retained for category",
            )
        elif pd.notna(row["retention_days"]) and row["age_days"] > float(
            row["retention_days"]
        ):
            action, reason = (
                "quarantine_candidate",
                f"age exceeds retention_days={int(row['retention_days'])}",
            )
        elif compactable.loc[index]:
            count = compact_counts.get((row["category"], row["parent_path"]), 0)
            if count >= row["min_compaction_files"]:
                action, reason = (
                    "compact_candidate",
                    f"{count} small homogeneous files exceed compact_after_days",
                )
            else:
                action, reason = (
                    "retain",
                    f"only {count} compaction candidates; minimum is {row['min_compaction_files']}",
                )
        else:
            action, reason = "retain", "within retention and compaction thresholds"
        actions.append(action)
        reasons.append(reason)
    plan["planned_action"] = actions
    plan["action_reason"] = reasons
    if not set(actions).issubset(PLANNED_ACTIONS):
        raise EvidenceLifecycleError("Lifecycle planner produced an unsupported action.")
    plan["requires_explicit_apply"] = plan["planned_action"] != "retain"
    plan["estimated_reclaimable_bytes"] = plan.apply(
        lambda row: int(row["size_bytes"])
        if row["planned_action"] == "quarantine_candidate"
        else 0,
        axis=1,
    )
    material = {
        "policy_contract_version": policy["contract_version"],
        "policy_sha256": policy["policy_sha256"],
        "plan_created_at_utc": created_at.isoformat(),
        "rows": [
            {
                "relative_path": row.relative_path,
                "sha256": row.sha256,
                "planned_action": row.planned_action,
            }
            for row in plan.sort_values("relative_path").itertuples(index=False)
        ],
    }
    plan_id = "elp-" + _digest(material)[:24]
    plan["plan_id"] = plan_id
    plan["plan_created_at_utc"] = created_at
    plan["plan_contract_version"] = PLAN_CONTRACT_VERSION
    total_bytes = int(plan["size_bytes"].sum())
    reclaimable = int(plan["estimated_reclaimable_bytes"].sum())
    compact_bytes = int(
        plan.loc[plan["planned_action"] == "compact_candidate", "size_bytes"].sum()
    )
    gib = 1024**3
    summary = {
        "plan_id": plan_id,
        "plan_created_at_utc": created_at.isoformat(),
        "inventory_as_of_utc": pd.Timestamp(
            plan["inventory_as_of_utc"].iloc[0]
        ).isoformat(),
        "policy_contract_version": policy["contract_version"],
        "policy_sha256": policy["policy_sha256"],
        "inventory_file_count": int(len(plan)),
        "inventory_size_bytes": total_bytes,
        "inventory_size_gib": total_bytes / gib,
        "retain_file_count": int((plan["planned_action"] == "retain").sum()),
        "compact_candidate_file_count": int(
            (plan["planned_action"] == "compact_candidate").sum()
        ),
        "compact_candidate_bytes": compact_bytes,
        "quarantine_candidate_file_count": int(
            (plan["planned_action"] == "quarantine_candidate").sum()
        ),
        "estimated_reclaimable_bytes": reclaimable,
        "estimated_reclaimable_gib": reclaimable / gib,
        "protected_file_count": int(
            (plan["always_protect"] | plan["protected_by_candidate"]).sum()
        ),
        "unclassified_file_count": int((~plan["classified"]).sum()),
        "monthly_storage_cost_per_gib": cost,
        "estimated_current_monthly_storage_cost": total_bytes / gib * cost,
        "estimated_reclaimable_monthly_storage_cost": reclaimable / gib * cost,
        "mutation_performed": False,
        "summary_contract_version": SUMMARY_CONTRACT_VERSION,
    }
    return plan.reset_index(drop=True), summary
