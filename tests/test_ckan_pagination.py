import pytest

from ingestion.common.api_client import CkanPaginationError, fetch_ckan_resource


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _payload(records, total, resource_id="resource-123"):
    return {
        "help": "https://example.test/api/3/action/help_show?name=datastore_search",
        "success": True,
        "result": {
            "resource_id": resource_id,
            "records": records,
            "limit": 2,
            "total": total,
        },
    }


def test_fetch_ckan_resource_reassembles_complete_deterministic_snapshot():
    calls = []
    pages = {
        0: _payload([{"_id": 1}, {"_id": 2}], total=3),
        2: _payload([{"_id": 3}], total=4),
    }

    def fake_get(url, params, headers, timeout):
        calls.append(
            {
                "url": url,
                "params": dict(params),
                "headers": headers,
                "timeout": timeout,
            }
        )
        return FakeResponse(pages[params["offset"]])

    result = fetch_ckan_resource(
        url="https://example.test/api/3/action/datastore_search",
        params={"resource_id": "resource-123", "limit": 999},
        headers={"Authorization": "secret"},
        timeout_seconds=15,
        page_size=2,
        max_records=10,
        request_get=fake_get,
    )

    assert [record["_id"] for record in result["result"]["records"]] == [1, 2, 3]
    assert [call["params"]["offset"] for call in calls] == [0, 2]
    assert all(call["params"]["sort"] == "_id asc" for call in calls)
    assert result["result"]["pagination"] == {
        "complete": True,
        "page_size": 2,
        "page_count": 2,
        "records_fetched": 3,
        "source_total_at_start": 3,
        "source_total_at_finish": 4,
        "sort": "_id asc",
    }


def test_fetch_ckan_resource_fails_before_truncating_configured_bound():
    def fake_get(url, params, headers, timeout):
        return FakeResponse(_payload([{"_id": 1}, {"_id": 2}], total=6))

    with pytest.raises(CkanPaginationError, match="above max_records=5"):
        fetch_ckan_resource(
            url="https://example.test/api/3/action/datastore_search",
            params={"resource_id": "resource-123"},
            page_size=2,
            max_records=5,
            request_get=fake_get,
        )


def test_fetch_ckan_resource_rejects_empty_page_before_snapshot_complete():
    pages = {
        0: _payload([{"_id": 1}, {"_id": 2}], total=3),
        2: _payload([], total=3),
    }

    def fake_get(url, params, headers, timeout):
        return FakeResponse(pages[params["offset"]])

    with pytest.raises(CkanPaginationError, match="empty page"):
        fetch_ckan_resource(
            url="https://example.test/api/3/action/datastore_search",
            params={"resource_id": "resource-123"},
            page_size=2,
            max_records=10,
            request_get=fake_get,
        )


def test_fetch_ckan_resource_rejects_duplicate_or_reordered_ids():
    pages = {
        0: _payload([{"_id": 1}, {"_id": 2}], total=3),
        2: _payload([{"_id": 2}], total=3),
    }

    def fake_get(url, params, headers, timeout):
        return FakeResponse(pages[params["offset"]])

    with pytest.raises(CkanPaginationError, match="appeared on more than one page"):
        fetch_ckan_resource(
            url="https://example.test/api/3/action/datastore_search",
            params={"resource_id": "resource-123"},
            page_size=2,
            max_records=10,
            request_get=fake_get,
        )
