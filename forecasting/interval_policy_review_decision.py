from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DECISION_CONTRACT_VERSION = "interval-policy-review-decision-v1"
SENSITIVITY_CONTRACT_VERSION = "interval-policy-sensitivity-v1"
DECISION_ID_PATTERN = re.compile(r"^ipd-[0-9a-f]{24}$")
SENSITIVITY_RUN_ID_PATTERN = re.compile(r"^ips-[0-9a-f]{24}$")
TREND_RUN_ID_PATTERN = re.compile(r"^iht-[0-9a-f]{24}$")
ALLOWED_DECISIONS = {
    "retain_active_policy",
    "reject_candidate",
    "request_candidate_revision",
}
DECISION_EFFECTS = {
    "retain_active_policy": "active_policy_retained",
    "reject_candidate": "candidate_rejected",
    "request_candidate_revision": "candidate_revision_required",
}
STATUSES = {"healthy", "warning", "failed"}
AUTHORITY_FIELDS = (
    "active_policy_updated",
    "candidate_thresholds_activated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
)
DECISION_SAFETY_FIELDS = (
    "threshold_activation_authorized",
    "active_policy_updated",
    "candidate_thresholds_activated",
    "retained_evidence_mutated",
    "interval_recalibration_performed",
    "model_change_performed",
    "schedule_change_performed",
    "promotion_change_performed",
    "alert_delivery_performed",
    "deployment_performed",
    "external_publication_performed",
)
REQUIRED_SUMMARY_COLUMNS = {
    "sensitivity_run_id",
    "sensitivity_run_timestamp_utc",
    "trend_run_id",
    "scenario",
    "candidate_id",
    "candidate_role",
    "candidate_version",
    "retained_monitor_status",
    "active_reference_status",
    "candidate_status",
    "status_changed_from_active",
    "sensitivity_classification",
    "slice_count",
    "changed_slice_count",
    "human_review_required",
    "sensitivity_contract_version",
    *AUTHORITY_FIELDS,
}


class IntervalPolicyReviewDecisionError(ValueError):
    """Raised when a policy-review decision is malformed or unsafe."""


def _required_text(value: Any, name: str) -> str:
    if value is None:
        raise IntervalPolicyReviewDecisionError(f"{name} must be non-empty.")
    text = str(value).strip()
    if not text:
        raise IntervalPolicyReviewDecisionError(f"{name} must be non-empty.")
    return text


def _utc_timestamp(value: Any, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be a valid timezone-aware timestamp."
        ) from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be timezone-aware."
        )
    return timestamp.tz_convert("UTC")


def _boolean(series: pd.Series, name: str) -> pd.Series:
    if series.dtype == bool:
        return series.astype(bool)
    parsed = series.astype(str).str.strip().str.lower().map(
        {"true": True, "false": False}
    )
    if parsed.isna().any():
        raise IntervalPolicyReviewDecisionError(
            f"{name} must contain boolean values."
        )
    return parsed.astype(bool)


def _non_negative_integer(series: pd.Series, name: str) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if values.isna().any() or (values < 0).any() or not (values % 1 == 0).all():
        raise IntervalPolicyReviewDecisionError(
            f"{name} must contain non-negative integers."
        )
    return values.astype(int)


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return _utc_timestamp(value, "timestamp").isoformat()
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
    encoded = json.dumps(
        _canonical(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _unique_texts(
    values: Iterable[Any], name: str, *, required: bool
) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
        raise IntervalPolicyReviewDecisionError(
            f"{name} must be a list of strings."
        )
    result = [_required_text(value, name) for value in values]
    if required and not result:
        raise IntervalPolicyReviewDecisionError(
            f"{name} must contain at least one item."
        )
    if len(set(result)) != len(result):
        raise IntervalPolicyReviewDecisionError(
            f"{name} must not contain duplicates."
        )
    return result


def prepare_sensitivity_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate one complete retained G26 sensitivity summary."""
    missing = sorted(REQUIRED_SUMMARY_COLUMNS - set(frame.columns))
    if missing:
        raise IntervalPolicyReviewDecisionError(
            "Sensitivity summary is missing required columns: "
            + ", ".join(missing)
            + "."
        )
    prepared = frame.copy()
    text_columns = (
        "sensitivity_run_id",
        "trend_run_id",
        "scenario",
        "candidate_id",
        "candidate_role",
        "candidate_version",
        "retained_monitor_status",
        "active_reference_status",
        "candidate_status",
        "sensitivity_classification",
        "sensitivity_contract_version",
    )
    for column in text_columns:
        prepared[column] = prepared[column].map(
            lambda value, name=column: _required_text(value, name)
        )
    prepared["sensitivity_run_timestamp_utc"] = prepared[
        "sensitivity_run_timestamp_utc"
    ].map(lambda value: _utc_timestamp(value, "sensitivity_run_timestamp_utc"))
    for column in (
        "status_changed_from_active",
        "human_review_required",
        *AUTHORITY_FIELDS,
    ):
        prepared[column] = _boolean(prepared[column], column)
    prepared["slice_count"] = _non_negative_integer(
        prepared["slice_count"], "slice_count"
    )
    prepared["changed_slice_count"] = _non_negative_integer(
        prepared["changed_slice_count"], "changed_slice_count"
    )
    if (prepared["slice_count"] < 1).any():
        raise IntervalPolicyReviewDecisionError(
            "slice_count must contain positive integers."
        )
    if (prepared["changed_slice_count"] > prepared["slice_count"]).any():
        raise IntervalPolicyReviewDecisionError(
            "changed_slice_count cannot exceed slice_count."
        )
    if prepared["sensitivity_run_id"].nunique() != 1:
        raise IntervalPolicyReviewDecisionError(
            "Exactly one sensitivity_run_id is required."
        )
    sensitivity_run_id = prepared["sensitivity_run_id"].iloc[0]
    if not SENSITIVITY_RUN_ID_PATTERN.fullmatch(sensitivity_run_id):
        raise IntervalPolicyReviewDecisionError(
            "sensitivity_run_id is malformed."
        )
    if prepared["sensitivity_run_timestamp_utc"].nunique²È="25‘¥‘…Ñ•}¥ˆ(€€€€¤(€€€Ñ…É•Ð€ôÁÉ•Á…É•¹±½mÁÉ•Á…É•‘l‰…¹‘¥‘…Ñ•}¥‰t€ôôÑ…É•Ñ}¥‘t(€€€¥˜Ñ…É•Ð¹•µÁÑäè(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰•¥Í¥½¸Ñ…É•Ð¥Ì…‰Í•¹Ð™É½´Í•¹Í¥Ñ¥Ù¥Ñä•Ù¥‘•¹”¸ˆ(€€€€€€€€¤(€€€¥˜‘•¥Í¥½¸¹•Ð ‰Ñ…É•Ñ}…¹‘¥‘…Ñ•}É½±”ˆ¤€„ôÑ…É•Ñl(€€€€€€€€‰…¹‘¥‘…Ñ•}É½±”ˆ(€€€t¹¥±½lÁtè(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰•¥Í¥½¸Ñ…É•ÐÉ½±”¥Ì¥¹½¹Í¥ÍÑ•¹Ð¸ˆ(€€€€€€€€¤(€€€¥˜‘•¥Í¥½¸¹•Ð ‰Ñ…É•Ñ}…¹‘¥‘…Ñ•}Ù•ÉÍ¥½¸ˆ¤€„ôÑ…É•Ñl(€€€€€€€€‰…¹‘¥‘…Ñ•}Ù•ÉÍ¥½¸ˆ(€€€t¹¥±½lÁtè(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰•¥Í¥½¸Ñ…É•ÐÙ•ÉÍ¥½¸¥Ì¥¹½¹Í¥ÍÑ•¹Ð¸ˆ(€€€€€€€€¤(€€€¡…¹•Ì€ô}Õ¹¥ÅÕ•}Ñ•áÑÌ (€€€€€€€‘•¥Í¥½¸¹•Ð ‰É•ÅÕ•ÍÑ•‘}¡…¹•Ìˆ°€ ¤¤°(€€€€€€€€‰É•ÅÕ•ÍÑ•‘}¡…¹•Ìˆ°(€€€€€€€É•ÅÕ¥É•õ‘•¥Í¥½¹}¹…µ”€ôô€‰É•ÅÕ•ÍÑ}…¹‘¥‘…Ñ•}É•Ù¥Í¥½¸ˆ°(€€€€¤(€€€¥˜‘•¥Í¥½¹}¹…µ”€ôô€‰É•Ñ…¥¹}…Ñ¥Ù•}Á½±¥äˆè(€€€€€€€¥˜Ñ…É•Ñ}¥€„ô€‰…Ñ¥Ù”µÉ•™•É•¹”ˆ½ÈÑ…É•Ñl‰…¹‘¥‘…Ñ•}É½±”‰t¹¥±½lÁt€„ô€‰…Ñ¥Ù•}É•™•É•¹”ˆ½È¡…¹•Ìè(€€€€€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€€€€€‰I•Ñ…¥¸‘•¥Í¥½¸™¥•±‘Ì…É”¥¹½¹Í¥ÍÑ•¹Ð¸ˆ(€€€€€€€€€€€€¤(€€€•±¥˜‘•¥Í¥½¹}¹…µ”€ôô€‰É•©•Ñ}…¹‘¥‘…Ñ”ˆè(€€€€€€€¥˜Ñ…É•Ñl‰…¹‘¥‘…Ñ•}É½±”‰t¹¥±½lÁt€„ô€‰É•Ù¥•Ý}…¹‘¥‘…Ñ”ˆ½È¡…¹•Ìè(€€€€€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹É½È (€€€€€€€€€€€€€€€€‰I•©•Ð‘•¥Í¥½¸™¥•±‘Ì…É”¥¹½¹Í¥ÍÑ•¹Ð¸ˆ(€€€€€€€€€€€€¤(€€€•±¥˜Ñ…É•Ñl‰…¹‘¥‘…Ñ•}É½±”‰t¹¥±½lÁt€„ô€‰É•Ù¥•Ý}…¹‘¥‘…Ñ”ˆ½È¹½Ð¡…¹•Ì€è(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰I•Ù¥Í¥½¸‘•¥Í¥½¸™¥•±‘Ì…É”¥¹½¹Í¥ÍÑ•¹Ð¸ˆ(€€€€€€€€¤(€€€}É•ÅÕ¥É•‘}Ñ•áÐ¡‘•¥Í¥½¸¹•Ð ‰É•Ù¥•Ý•É}¹…µ”ˆ¤°€‰É•Ù¥•Ý•É}¹…µ”ˆ¤(€€€}É•ÅÕ¥É•‘}Ñ•áÐ¡‘•¥Í¥½¸¹•Ð ‰É•Ù¥•Ý•É}É½±”ˆ¤°€‰É•Ù¥•Ý•É}É½±”ˆ¤(€€€}É•ÅÕ¥É•‘}Ñ•áÐ¡‘•¥Í¥½¸¹•Ð ‰É•Ù¥•Ý}Ñ¥­•Ðˆ¤°€‰É•Ù¥•Ý}Ñ¥­•Ðˆ¤(€€€}É•ÅÕ¥É•‘}Ñ•áÐ¡‘•¥Í¥½¸¹•Ð ‰É…Ñ¥½¹…±”ˆ¤°€‰É…Ñ¥½¹…±”ˆ¤(€€€‘•¥Í¥½¹}Ñ¥µ•ÍÑ…µÀ€ô}ÕÑ}Ñ¥µ•ÍÑ…µÀ (€€€€€€€‘•¥Í¥½¸¹•Ð ‰‘•¥Í¥½¹}Ñ¥µ•ÍÑ…µÁ}ÕÑŒˆ¤°€‰‘•¥Í¥½¹}Ñ¥µ•ÍÑ…µÁ}ÕÑŒˆ(€€€€¤(€€€¥˜‘•¥Í¥½¹}Ñ¥µ•ÍÑ…µÀ€ðÁÉ•Á…É•‘l‰Í•¹Í¥Ñ¥Ù¥Ñå}ÉÕ¹}Ñ¥µ•ÍÑ…µÁ}ÕÑŒ‰t¹¥±½lÁtè(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰•¥Í¥½¸Ñ¥µ•ÍÑ…µÀÁÉ••‘•ÌÍ•¹Í¥Ñ¥Ù¥Ñä•Ù¥‘•¹”¸ˆ(€€€€€€€€¤(€€€•áÁ•Ñ•‘}Í•¹…É¥½Ì€ômt(€€€™½ÈÉ½Ü¥¸Ñ…É•Ð¹Í½ÉÑ}Ù…±Õ•Ì ‰Í•¹…É¥¼ˆ¤¹¥Ñ•ÉÑÕÁ±•Ì¡¥¹‘•àõ…±Í”¤è(€€€€€€€•áÁ•Ñ•‘}Í•¹…É¥½Ì¹…ÁÁ•¹ (€€€€€€€€€€€ì(€€€€€€€€€€€€€€€€‰Í•¹…É¥¼ˆèÉ½Ü¹Í•¹…É¥¼°(€€€€€€€€€€€€€€€€‰É•Ñ…¥¹•‘}µ½¹¥Ñ½É}ÍÑ…ÑÕÌˆèÉ½Ü¹É•Ñ…¥¹•‘}µ½¹¥Ñ½É}ÍÑ…ÑÕÌ°(€€€€€€€€€€€€€€€€‰…Ñ¥Ù•}É•™•É•¹•}ÍÑ…ÑÕÌˆèÉ½Ü¹…Ñ¥Ù•}É•™•É•¹•}ÍÑ…ÑÕÌ°(€€€€€€€€€€€€€€€€‰Ñ…É•Ñ}…¹‘¥‘…Ñ•}ÍÑ…ÑÕÌˆèÉ½Ü¹…¹‘¥‘…Ñ•}ÍÑ…ÑÕÌ°(€€€€€€€€€€€€€€€€‰Í•¹Í¥Ñ¥Ù¥Ñå}±…ÍÍ¥™¥…Ñ¥½¸ˆèÉ½Ü¹Í•¹Í¥Ñ¥Ù¥Ñå}±…ÍÍ¥™¥…Ñ¥½¸°(€€€€€€€€€€€€€€€€‰ÍÑ…ÑÕÍ}¡…¹•‘}™É½µ}…Ñ¥Ù”ˆè‰½½° (€€€€€€€€€€€€€€€€€€€É½Ü¹ÍÑ…ÑÕÍ}¡…¹•‘}™É½µ}…Ñ¥Ù”(€€€€€€€€€€€€€€€€¤°(€€€€€€€€€€€€€€€€‰¡…¹•‘}Í±¥•}½Õ¹Ðˆè¥¹Ð¡É½Ü¹¡…¹•‘}Í±¥•}½Õ¹Ð¤°(€€€€€€€€€€€€€€€€‰¡Õµ…¹}É•Ù¥•Ý}É•ÅÕ¥É•ˆè‰½½°¡É½Ü¹¡Õµ…¹}É•Ù¥•Ý}É•ÅÕ¥É•¤°(€€€€€€€€€€€ô(€€€€€€€€¤(€€€¥˜}…¹½¹¥…°¡‘•¥Í¥½¸¹•Ð ‰Í•¹…É¥½}•Ù¥‘•¹”ˆ¤¤€„ô}…¹½¹¥…° (€€€€€€€•áÁ•Ñ•‘}Í•¹…É¥½Ì(€€€€¤è(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰•¥Í¥½¸Í•¹…É¥¼•Ù¥‘•¹”¥Ì¥¹½¹Í¥ÍÑ•¹Ð¸ˆ(€€€€€€€€¤(€€€¥˜‘•¥Í¥½¸¹•Ð ‰¹…µ•‘}¡Õµ…¹}É•Ù¥•Ý}½¹™¥Éµ•ˆ¤¥Ì¹½ÐQÉÕ”è(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰9…µ•¡Õµ…¸É•Ù¥•ÜµÕÍÐ‰”½¹™¥Éµ•¸ˆ(€€€€€€€€¤(€€€•áÁ•Ñ•‘}™½±±½Ý}ÕÀ€ô‘•¥Í¥½¹}¹…µ”€ôô€‰É•ÅÕ•ÍÑ}…¹‘¥‘…Ñ•}É•Ù¥Í¥½¸ˆ(€€€¥˜‘•¥Í¥½¸¹•Ð ‰™½±±½Ý}ÕÁ}¡Õµ…¹}…Ñ¥½¹}É•ÅÕ¥É•ˆ¤¥Ì¹½Ð•áÁ•Ñ•‘}™½±±½Ý}ÕÀè(€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€‰™½±±½Ý}ÕÁ}¡Õµ…¹}…Ñ¥½¹}É•ÅÕ¥É•¥Ì¥¹½¹Í¥ÍÑ•¹Ð¸ˆ(€€€€€€€€¤(€€€™½È™¥•±¥¸%M%=9}MQe}%1Lè(€€€€€€€¥˜‘•¥Í¥½¸¹•Ð¡™¥•±¤¥Ì¹½Ð…±Í”è(€€€€€€€€€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€€€€€€€€˜‰•¥Í¥½¸Í…™•Ñä™¥•±í™¥•±‘ôµÕÍÐ‰”™…±Í”¸ˆ(€€€€€€€€€€€€¤(()‘•˜É•¹‘•É}Á½±¥å}É•Ù¥•Ý}‘•¥Í¥½¸¡‘•¥Í¥½¸è‘¥ÑmÍÑÈ°¹åt¤€´øÍÑÈè(€€€€ˆˆ‰I•¹‘•ÈÑ¡”¥µµÕÑ…‰±”‘•¥Í¥½¸…Ì¡Õµ…¸µÉ•…‘…‰±”5…É­‘½Ý¸¸ˆˆˆ(€€€±¥¹•Ì€ôl(€€€€€€€€ˆŒ%¹Ñ•ÉÙ…°µµ½¹¥Ñ½É¥¹œÁ½±¥äÉ•Ù¥•Ü‘•¥Í¥½¸ˆ°(€€€€€€€€ˆˆ°(€€€€€€€˜ˆ´•¥Í¥½¸%èí‘•¥Í¥½¹l‘•¥Í¥½¹}¥uõ€ˆ°(€€€€€€€˜ˆ´M•¹Í¥Ñ¥Ù¥ÑäÉÕ¸èí‘•¥Í¥½¹lÍ•¹Í¥Ñ¥Ù¥Ñå}ÉÕ¹}¥uõ€ˆ°(€€€€€€€˜ˆ´•¥Í¥½¸èí‘•¥Í¥½¹l‘•¥Í¥½¸uõ€ˆ°(€€€€€€€˜ˆ´Q…É•Ð…¹‘¥‘…Ñ”èí‘•¥Í¥½¹lÑ…É•Ñ}…¹‘¥‘…Ñ•}¥uõ€ˆ°(€€€€€€€˜ˆ´I•Ù¥•Ý•Èèí‘•¥Í¥½¹lÉ•Ù¥•Ý•É}¹…µ”uô€¡í‘•¥Í¥½¹lÉ•Ù¥•Ý•É}É½±”uô¤ˆ°(€€€€€€€˜ˆ´I•Ù¥•ÜÑ¥­•Ðèí‘•¥Í¥½¹lÉ•Ù¥•Ý}Ñ¥­•Ðuõ€ˆ°(€€€€€€€˜ˆ´•¥Í¥½¸Ñ¥µ”èí‘•¥Í¥½¹l‘•¥Í¥½¹}Ñ¥µ•ÍÑ…µÁ}ÕÑŒuõ€ˆ°(€€€€€€€€ˆˆ°(€€€€€€€€ˆŒŒI…Ñ¥½¹…±”ˆ°(€€€€€€€€ˆˆ°(€€€€€€€‘•¥Í¥½¹l‰É…Ñ¥½¹…±”‰t°(€€€€€€€€ˆˆ°(€€€t(€€€¥˜‘•¥Í¥½¹l‰É•ÅÕ•ÍÑ•‘}¡…¹•Ì‰tè(€€€€€€€±¥¹•Ì¹•áÑ•¹¡lˆŒŒI•ÅÕ•ÍÑ•¡…¹•Ìˆ°€ˆ‰t¤(€€€€€€€±¥¹•Ì¹•áÑ•¹¡˜ˆ´í¥Ñ•µôˆ™½È¥Ñ•´¥¸‘•¥Í¥½¹l‰É•ÅÕ•ÍÑ•‘}¡…¹•Ì‰t¤(€€€€€€€±¥¹•Ì¹…ÁÁ•¹ ˆˆ¤(€€€±¥¹•Ì¹•áÑ•¹ (€€€€€€€l(€€€€€€€€€€€€ˆŒŒI•Ñ…¥¹•Í•¹…É¥¼•Ù¥‘•¹”ˆ°(€€€€€€€€€€€€ˆˆ°(€€€€€€€€€€€€‰ðM•¹…É¥¼ðI•Ñ…¥¹•ðÑ¥Ù”É•™•É•¹”ðQ…É•Ð…¹‘¥‘…Ñ”ð±…ÍÍ¥™¥…Ñ¥½¸ð¡…¹•Í±¥•Ìðˆ°(€€€€€€€€€€€€‰ð€´´´ð€´´´ð€´´´ð€´´´ð€´´´ð€´´´èðˆ°(€€€€€€€t(€€€€¤(€€€™½ÈÉ½Ü¥¸‘•¥Í¥½¹l‰Í•¹…É¥½}•Ù¥‘•¹”‰tè(€€€€€€€±¥¹•Ì¹…ÁÁ•¹ (€€€€€€€€€€€˜‰ðíÉ½ÝlÍ•¹…É¥¼uôðíÉ½ÝlÉ•Ñ…¥¹•‘}µ½¹¥Ñ½É}ÍÑ…ÑÕÌuôð€ˆ(€€€€€€€€€€€˜‰íÉ½Ýl…Ñ¥Ù•}É•™•É•¹•}ÍÑ…ÑÕÌuôðíÉ½ÝlÑ…É•Ñ}…¹‘¥‘…Ñ•}ÍÑ…ÑÕÌuôð€ˆ(€€€€€€€€€€€˜‰íÉ½ÝlÍ•¹Í¥Ñ¥Ù¥Ñå}±…ÍÍ¥™¥…Ñ¥½¸uôðíÉ½Ýl¡…¹•‘}Í±¥•}½Õ¹Ðuôðˆ(€€€€€€€€¤(€€€±¥¹•Ì¹•áÑ•¹ (€€€€€€€l(€€€€€€€€€€€€ˆˆ°(€€€€€€€€€€€€‰Q¡¥ÌÉ••¥ÁÐÉ•½É‘Ì¡Õµ…¸É•Ù¥•Ü•Ù¥‘•¹”½¹±ä¸%Ð‘½•Ì¹½Ð…Ñ¥Ù…Ñ”…¹‘¥‘…Ñ”Ñ¡É•Í¡½±‘Ì½ÈÕÁ‘…Ñ”Ñ¡”…Ñ¥Ù”µ½¹¥Ñ½É¥¹œÁ½±¥ä¸ˆ°(€€€€€€€€€€€€‰9¼¥¹Ñ•ÉÙ…°É•…±¥‰É…Ñ¥½¸°µ½‘•°¡…¹”°Í¡•‘Õ±”¡…¹”°ÁÉ½µ½Ñ¥½¸°…±•ÉÐ‘•±¥Ù•Éä°‘•Á±½åµ•¹Ð°½È•áÑ•É¹…°ÁÕ‰±¥…Ñ¥½¸¥ÌÁ•É™½Éµ•¸ˆ°(€€€€€€€€€€€€ˆˆ°(€€€€€€€t(€€€€¤(€€€É•ÑÕÉ¸€‰q¸ˆ¹©½¥¸¡±¥¹•Ì¤(()‘•˜É•…‘}™É…µ”¡Á…Ñ èA…Ñ ¤€´øÁ¹…Ñ…É…µ”è(€€€ÍÕ™™¥à€ôA…Ñ ¡Á…Ñ ¤¹ÍÕ™™¥à¹±½Ý•È ¤(€€€¥˜ÍÕ™™¥à€ôô€ˆ¹ÍØˆè(€€€€€€€É•ÑÕÉ¸Á¹É•…‘}ÍØ¡Á…Ñ ¤(€€€¥˜ÍÕ™™¥à¥¸ìˆ¹Á…ÉÅÕ•Ðˆ°€ˆ¹ÁÄ‰ôè(€€€€€€€É•ÑÕÉ¸Á¹É•…‘}Á…ÉÅÕ•Ð¡Á…Ñ ¤(€€€É…¥Í”%¹Ñ•ÉÙ…±A½±¥åI•Ù¥•Ý•¥Í¥½¹ÉÉ½È (€€€€€€€€‰M•¹Í¥Ñ¥Ù¥ÑäÍÕµµ…Éä¥¹ÁÕÐµÕÍÐ‰”MX½ÈA…ÉÅÕ•Ð¸ˆ(€€€€¤(()‘•˜ÝÉ¥Ñ•}Á½±¥å}É•Ù¥•Ý}‘•¥Í¥½¸ (€€€½ÕÑÁÕÑ}‘¥É•Ñ½ÉäèA…Ñ °(€€€‘•¥Í¥½¸è‘¥ÑmÍÑÈ°¹åt°(€€€Í•¹Í¥Ñ¥Ù¥Ñå}ÍÕµµ…ÉäèÁ¹…Ñ…É…µ”°(¤€´øÑÕÁ±•mA…Ñ °A…Ñ¡tè(€€€€ˆˆ‰]É¥Ñ”¥µµÕÑ…‰±”)M=8…¹5…É­‘½Ý¸‘•¥Í¥½¸•Ù¥‘•¹”¸ˆˆˆ(€€€Ù•É¥™å}Á½±¥å}É•Ù¥•Ý}‘•¥Í¥½¸¡‘•¥Í¥½¸°Í•¹Í¥Ñ¥Ù¥Ñå}ÍÕµµ…Éä¤(€€€½ÕÑÁÕÑ}‘¥É•Ñ½Éä€ôA…Ñ ¡½ÕÑÁÕÑ}‘¥É•Ñ½Éä¤(€€€½ÕÑÁÕÑ}‘¥É•Ñ½Éä¹µ­‘¥È¡Á…É•¹ÑÌõQÉÕ”°•á¥ÍÑ}½¬õQÉÕ”¤(€€€©Í½¹}Á…Ñ €ô½ÕÑÁÕÑ}‘¥É•Ñ½Éä€¼˜‰¥¹Ñ•ÉÙ…±}Á½±¥å}É•Ù¥•Ý}‘•¥Í¥½¹}í‘•¥Í¥½¹l‘•¥Í¥½¹}¥uô¹©Í½¸ˆ(€€€µ…É­‘½Ý¹}Á…Ñ €ô½ÕÑÁÕÑ}‘¥É•Ñ½Éä€¼˜‰¥¹Ñ•ÉÙ…±}Á½±¥å}É•Ù¥•Ý}‘•¥Í¥½¹}í‘•¥Í¥½¹l‘•¥Í¥½¹}¥uô¹µˆ(€€€Ñ•µÁ½É…Éå}Á…Ñ¡Ì€ôl(€€€€€€€©Í½¹}Á…Ñ ¹Ý¥Ñ¡}¹…µ”¡˜ˆ¹í©Í½¹}Á…Ñ ¹¹…µ•ô¹ÑµÀˆ¤°(€€€€€€€µ…É­‘½Ý¹}Á…Ñ ¹Ý¥Ñ¡}¹…µ”¡˜ˆ¹íµ…É­‘½Ý¹}Á…Ñ ¹¹…µ•ô¹ÑµÀˆ¤°(€€€t(€€€™½È…¹‘¥‘…Ñ”¥¸€¡©Í½¹}Á…Ñ °µ…É­‘½Ý¹}Á…Ñ °€©Ñ•µÁ½É…Éå}Á…Ñ¡Ì¤è(€€€€€€€¥˜…¹‘¥‘…Ñ”¹•á¥ÍÑÌ ¤è(€€€€€€€€€€€É…¥Í”¥±•á¥ÍÑÍÉÉ½È¡˜‰I•™ÕÍ¥¹œÑ¼½Ù•ÉÝÉ¥Ñ”í…¹‘¥‘…Ñ•ô¸ˆ¤(€€€ÑÉäè(€€€€€€€Ñ•µÁ½É…Éå}Á…Ñ¡ÍlÁt¹ÝÉ¥Ñ•}Ñ•áÐ (€€€€€€€€€€€©Í½¸¹‘ÕµÁÌ¡}…¹½¹¥…°¡‘•¥Í¥½¸¤°¥¹‘•¹ÐôÈ°Í½ÉÑ}­•åÌõQÉÕ”¤€¬€‰q¸ˆ°(€€€€€€€€€€€•¹½‘¥¹œô‰ÕÑ˜´àˆ°(€€€€€€€€¤(€€€€€€€Ñ•µÁ½É…Éå}Á…Ñ¡ÍlÅt¹ÝÉ¥Ñ•}Ñ•áÐ (€€€€€€€€€€€É•¹‘•É}Á½±¥å}É•Ù¥•Ý}‘•¥Í¥½¸¡‘•¥Í¥½¸¤°•¹½‘¥¹œô‰ÕÑ˜´àˆ(€€€€€€€€¤(€€€€€€€Ñ•µÁ½É…Éå}Á…Ñ¡ÍlÁt¹É•Á±…”¡©Í½¹}Á…Ñ ¤(€€€€€€€Ñ•µÁ½É…Éå}Á…Ñ¡ÍlÅt¹É•Á±…”¡µ…É­‘½Ý¹}Á…Ñ ¤(€€€™¥¹…±±äè(€€€€€€€™½ÈÑ•µÁ½É…Éä¥¸Ñ•µÁ½É…Éå}Á…Ñ¡Ìè(€€€€€€€€€€€Ñ•µÁ½É…Éä¹Õ¹±¥¹¬¡µ¥ÍÍ¥¹}½¬õQÉÕ”¤(€€€É•ÑÕÉ¸©Í½¹}Á…Ñ °µ…É­‘½Ý¹}Á…Ñ (