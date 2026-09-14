# Current-content ingestion contracts

`validate_payload` reads its contract as UTF-8 on every invocation. The helper
caches compilation by the decoded schema content, with at most eight compiled
entries, rather than caching the association between a filename and old rules.

Changing a contract at the same path therefore takes effect on the next call,
even when file size and modification time are unchanged. A stricter replacement
can reject data previously accepted; a relaxed replacement is no longer blocked
by the old validator. Identical schema text still reuses compilation. The cache
limits entry count, not total bytes, and is intended for trusted local schemas.

Schema JSON is parsed before Draft 2020-12 schema validation. Duplicate object
members at any depth are rejected, including differently escaped names that
decode to the same key. Non-standard `NaN`/`Infinity` constants and floating-point
overflow are rejected even in annotations such as `default`. Invalid JSON, invalid
UTF-8 and invalid schema definitions raise `ContractValidationError` identifying
the contract. A missing or unreadable file raises its filesystem error; there is
no fallback to previously cached content.

Valid object and boolean schemas, local `$defs` references, and existing payload
error reporting remain supported. Payloads are neither coerced nor rewritten.
No checked-in schema, format-assertion policy, external-reference behavior,
dependency or workflow is changed. Format annotation semantics are deliberately
not expanded by this cache correction.

## Operational boundary

Each invocation uses the content it read. This is not an atomic snapshot of a
file concurrently edited in place, or a fixed schema version across every page
of one ingestion run. Keep schemas immutable for the duration of a run when that
stronger guarantee is required. The helper does not authorize schema changes,
verify their author, access credentials, fetch a provider response, or publish
raw evidence. Existing reference-resolution behavior is not a network sandbox.

Tests in `tests/test_ingestion_contract_reload.py` reproduce stale-cache behavior,
malformed replacements, duplicate members and non-finite values, and preserve
valid caching, boolean schemas, local references and bounded error reporting.
The existing `tests/test_contract_validator.py` remains unchanged.

Parsing uses the standard-library hooks documented in the
[Python JSON reference](https://docs.python.org/3.11/library/json.html).
