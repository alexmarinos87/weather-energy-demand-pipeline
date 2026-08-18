from copy import deepcopy
from typing import Any, Callable

import requests


class CkanPaginationError(RuntimeError):
    """Raised when a CKAN resource cannot be retrieved as one complete snapshot."""


PayloadValidator = Callable[[dict[str, Any]], None]
RequestGet = Callable[..., Any]


def _require_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer.") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer.")
    return parsed


def _require_non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CkanPaginationError(f"{name} must be a non-negative integer.")
    return value


def fetch_ckan_resource(
    *,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 30,
    page_size: int = 1000,
    max_records: int = 50_000,
    validate_page: PayloadValidator | None = None,
    request_get: RequestGet = requests.get,
) -> dict[str, Any]:
    """Retrieve a deterministic, bounded CKAN DataStore snapshot.

    The first page fixes the snapshot target. Later rows appended by a live source
    are left for the next run, while a shrinking source or non-deterministic page
    order fails loudly rather than producing a partial raw capture.
    """
    page_size = _require_positive_int(page_size, "page_size")
    max_records = _require_positive_int(max_records, "max_records")
    timeout_seconds = _require_positive_int(timeout_seconds, "timeout_seconds")

    request_params = dict(params)
    expected_resource_id = request_params.get("resource_id")
    for controlled_parameter in ("limit", "offset", "sort"):
        request_params.pop(controlled_parameter, None)

    first_payload: dict[str, Any] | None = None
    snapshot_total: int | None = None
    latest_reported_total: int | None = None
    records: list[dict[str, Any]] = []
    seen_record_ids: set[int] = set()
    last_record_id: int | None = None
    page_count = 0

    while snapshot_total is None or len(records) < snapshot_total:
        remaining = max_records - len(records)
        if remaining < 1:
            raise CkanPaginationError(
                f"CKAN snapshot exceeded max_records={max_records}. "
                "Raise the configured bound deliberately or narrow the query."
            )

        if snapshot_total is None:
            requested_limit = min(page_size, remaining)
        else:
            requested_limit = min(page_size, snapshot_total - len(records), remaining)

        page_params = {
            **request_params,
            "limit": requested_limit,
            "offset": len(records),
            "sort": "_id asc",
        }
        response = request_get(
            url,
            params=page_params,
            headers=headers or {},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise CkanPaginationError("CKAN response must be a JSON object.")
        if validate_page is not None:
            validate_page(payload)

        result = payload.get("result")
        if not isinstance(result, dict):
            raise CkanPaginationError("CKAN response is missing result metadata.")

        resource_id = result.get("resource_id")
        if expected_resource_id and resource_id != expected_resource_id:
            raise CkanPaginationError(
                f"CKAN returned resource_id={resource_id!r}; "
                f"expected {expected_resource_id!r}."
            )

        reported_total = _require_non_negative_int(result.get("total"), "result.total")
        latest_reported_total = reported_total
        page_records = result.get("records")
        if not isinstance(page_records, list):
            raise CkanPaginationError("CKAN result.records must be an array.")

        if snapshot_total is None:
            snapshot_total = reported_total
            if snapshot_total > max_records:
                raise CkanPaginationError(
                    f"CKAN snapshot contains {snapshot_total} records, above "
                    f"max_records={max_records}. Raise the configured bound "
                    "deliberately or narrow the query."
                )
            first_payload = deepcopy(payload)
        elif reported_total < snapshot_total:
            raise CkanPaginationError(
                "CKAN result.total decreased during pagination; refusing an "
                "inconsistent raw snapshot."
            )

        if snapshot_total == 0:
            if page_records:
                raise CkanPaginationError(
                    "CKAN reported zero total records but returned data."
                )
            break

        if not page_records:
            raise CkanPaginationError(
                f"CKAN returned an empty page at offset {len(records)} before "
                f"the {snapshot_total}-record snapshot was complete."
            )
        if len(page_records) > requested_limit:
            raise CkanPaginationError(
                "CKAN returned more records than the requested page limit."
            )

        for record in page_records:
            if not isinstance(record, dict):
                raise CkanPaginationError("CKAN records must be JSON objects.")
            record_id = record.get("_id")
            if isinstance(record_id, bool) or not isinstance(record_id, int):
                raise CkanPaginationError("Each CKAN record must have an integer _id.")
            if record_id in seen_record_ids:
                raise CkanPaginationError(
                    f"CKAN record _id={record_id} appeared on more than one page."
                )
            if last_record_id is not None and record_id <= last_record_id:
                raise CkanPaginationError(
                    "CKAN pages were not strictly ordered by ascending _id."
                )
            seen_record_ids.add(record_id)
            last_record_id = record_id
            records.append(record)

        page_count += 1

    if first_payload is None or snapshot_total is None:
        raise CkanPaginationError("CKAN pagination completed without a response.")
    if len(records) != snapshot_total:
        raise CkanPaginationError(
            f"CKAN snapshot expected {snapshot_total} records but retrieved "
            f"{len(records)}."
        )

    combined_payload = first_payload
    combined_result = combined_payload["result"]
    combined_result["records"] = records
    combined_result["limit"] = page_size
    combined_result["offset"] = 0
    combined_result["total"] = snapshot_total
    combined_result["pagination"] = {
        "complete": True,
        "page_size": page_size,
        "page_count": page_count,
        "records_fetched": len(records),
        "source_total_at_start": snapshot_total,
        "source_total_at_finish": latest_reported_total,
        "sort": "_id asc",
    }
    return combined_payload
