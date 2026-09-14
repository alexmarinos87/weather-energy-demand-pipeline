# CKAN ingestion request and completion boundaries

`fetch_ckan_resource` validates `page_size`, `max_records` and
`timeout_seconds` before the first request. Accepted values are positive Python
integers or decimal integer strings, optionally surrounded by whitespace and
with a leading plus sign. Booleans, floating-point values (including `2.0`),
fractional strings and non-finite numbers are rejected rather than truncated.
Use YAML `page_size: 1000`, not `page_size: 1000.5` or `1000.0`.

A nonblank string `params.resource_id` is mandatory and is compared exactly with
every returned resource identity. Caller parameters are copied; limit, offset and
sort remain controlled by the paginator. Configuration errors raise `ValueError`
without a request. The optional page validator still runs before the helper's
built-in envelope checks, preserving its dataset-specific validation errors.

Every response must report the boolean `success: true`. HTTP 200 alone does not
establish CKAN action success. When `result.total_was_estimated` is present, it
must be the boolean `false`; estimated or malformed values are rejected on any
page, including an otherwise empty result. Older sources that omit estimation
metadata remain supported. Omission is not independent proof of an exact count.

The first reported total remains the target for this capture. Later appended
records are left for a separate run. Existing maximum-record, shrinking-total,
empty-page, duplicate-ID and ordering checks remain in force. No retry, new
request parameter, external source call or automatic scheduling is introduced.
HTTP exceptions continue to propagate rather than producing a partial return.

`pagination.complete` means that the checked first-page count was retrieved
under these constraints. Offset pagination does **not** provide a transactionally
frozen source: in-place edits or coordinated deletions/appends can evade these
checks. A trusted source snapshot/version would be needed for that stronger
claim. This function does not publish a raw file or alter any existing evidence.

Regression tests: `tests/test_ckan_request_boundaries.py` plus the existing
`tests/test_ckan_pagination.py`. All requests are injected test doubles.

References: [CKAN API guide](https://docs.ckan.org/en/2.11/api/) and
[DataStore response metadata](https://docs.ckan.org/en/2.11/maintaining/datastore.html).
