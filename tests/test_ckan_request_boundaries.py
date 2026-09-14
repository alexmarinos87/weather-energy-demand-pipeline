"""Boundary regressions using the real paginator and injected HTTP responses."""
from copy import deepcopy

import pytest
import requests

from ingestion.common.api_client import CkanPaginationError, fetch_ckan_resource

URL = "https://example.test/api/3/action/datastore_search"
RESOURCE = "resource-001"


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def page(ids=(), total=0):
    return {"success": True, "result": {
        "resource_id": RESOURCE, "records": [{"_id": key} for key in ids],
        "total": total, "limit": 2,
    }}


def run(get, **kwargs):
    options = {"url": URL, "params": {"resource_id": RESOURCE},
               "page_size": 2, "max_records": 10, "timeout_seconds": 30,
               "request_get": get}
    options.update(kwargs)
    return fetch_ckan_resource(**options)


@pytest.mark.parametrize("name", ["page_size", "max_records", "timeout_seconds"])
@pytest.mark.parametrize("value", [1.5, 2.0, float("nan"), float("inf"), True, None])
def test_invalid_bound_is_rejected_before_request(name, value):
    calls = []

    def get(*args, **kwargs):
        calls.append(kwargs)
        return Response(page())

    with pytest.raises(ValueError, match=name):
        run(get, **{name: value})
    assert calls == []


@pytest.mark.parametrize("name", ["page_size", "max_records", "timeout_seconds"])
@pytest.mark.parametrize("value", [1, 2, "2", " 002 "])
def test_integer_and_decimal_string_bounds_remain_usable(name, value):
    calls = []

    def get(*args, **kwargs):
        calls.append(kwargs)
        return Response(page())

    result = run(get, **{name: value})
    assert result["result"]["pagination"]["complete"] is True
    assert len(calls) == 1
    assert isinstance(calls[0]["timeout"], int)
    assert isinstance(calls[0]["params"]["limit"], int)


@pytest.mark.parametrize("identity", [None, "", " ", 12, False])
def test_resource_identity_is_required_before_request(identity):
    calls = []

    def get(*args, **kwargs):
        calls.append(kwargs)
        return Response(page())

    with pytest.raises(ValueError, match="resource_id"):
        run(get, params={"resource_id": identity})
    assert calls == []


def test_missing_resource_identity_is_rejected_before_request():
    calls = []
    with pytest.raises(ValueError, match="resource_id"):
        run(lambda *a, **kw: calls.append(kw) or Response(page()), params={})
    assert calls == []


@pytest.mark.parametrize("success", [False, None, 1, "true"])
def test_http_success_does_not_substitute_for_action_success(success):
    payload = page()
    payload["success"] = success
    with pytest.raises(CkanPaginationError, match="success"):
        run(lambda *a, **kw: Response(payload))


def test_missing_action_success_flag_is_rejected():
    payload = page()
    del payload["success"]
    with pytest.raises(CkanPaginationError, match="success"):
        run(lambda *a, **kw: Response(payload))


@pytest.mark.parametrize("estimated", [True, None, 1, "false"])
def test_estimated_or_malformed_total_flag_cannot_claim_completion(estimated):
    payload = page()
    payload["result"]["total_was_estimated"] = estimated
    with pytest.raises(CkanPaginationError, match="total_was_estimated"):
        run(lambda *a, **kw: Response(payload))


def test_explicit_exact_total_remains_supported():
    payload = page([1], 1)
    payload["result"]["total_was_estimated"] = False
    result = run(lambda *a, **kw: Response(payload))
    assert result["result"]["pagination"]["records_fetched"] == 1


@pytest.mark.parametrize("failure", ["success", "total_was_estimated"])
def test_every_page_must_satisfy_completion_contract(failure):
    pages = [page([1, 2], 3), page([3], 3)]
    if failure == "success":
        pages[1]["success"] = False
    else:
        pages[1]["result"]["total_was_estimated"] = True
    original = deepcopy(pages)
    calls = []

    def get(*args, **kwargs):
        calls.append(kwargs)
        return Response(pages[len(calls) - 1])

    with pytest.raises(CkanPaginationError, match=failure):
        run(get)
    assert len(calls) == 2
    assert pages == original


def test_growth_remains_bounded_by_first_page_and_inputs_are_unchanged():
    pages = {0: page([1, 2], 3), 2: page([3], 5)}
    before = deepcopy(pages)
    params = {"resource_id": RESOURCE, "limit": 999, "offset": 7, "sort": "other"}
    original_params = deepcopy(params)
    calls = []

    def get(*args, **kwargs):
        calls.append(deepcopy(kwargs))
        return Response(pages[kwargs["params"]["offset"]])

    result = run(get, params=params)
    assert [call["params"]["limit"] for call in calls] == [2, 1]
    assert all(call["params"]["sort"] == "_id asc" for call in calls)
    assert result["result"]["total"] == 3
    assert result["result"]["pagination"]["source_total_at_finish"] == 5
    assert pages == before and params == original_params


@pytest.mark.parametrize("error", [requests.Timeout, requests.HTTPError])
def test_http_failures_propagate_without_retry(error):
    calls = []

    def get(*args, **kwargs):
        calls.append(kwargs)
        raise error("injected transport failure")

    with pytest.raises(error, match="injected transport"):
        run(get)
    assert len(calls) == 1
