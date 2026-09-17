# Current source-area contract acceptance

`load_source_area_contract`, `resolve_source_area` and `validate_source_binding`
read the current UTF-8 catalogue for each call. Parsed and validated decoded
contents, not filenames or file timestamps, are cached in at most eight entries.
Editing a resource/city binding or removing an area takes effect on the next call,
even when file size and modification time are unchanged. Missing/unreadable files
and malformed replacements cannot fall back to a previously accepted mapping.

The catalogue must be a JSON object with a nonempty areas object. Duplicate JSON
members, non-finite constants and floating-point overflow are rejected before
acceptance. A contract version, normalized area key, display name, resource ID and
proxy city must be nonempty strings with no surrounding whitespace or control
characters. Numeric, boolean, null and container identities are not converted to
text. Ambiguous catalogue data must be corrected explicitly, not normalized into a
new identity during loading.

Existing caller source-area aliases, resource/city checks, finite coordinate
bounds and uniqueness checks are unchanged. Finite numeric coordinate strings
remain supported. The checked-in four-area mapping and valid metadata fields are
unchanged. Results are deep copies, so a caller cannot mutate a cached catalogue
through a returned object. Malformed JSON/object contracts raise `SourceAreaError`;
filesystem errors and invalid UTF-8 retain their original exception types.

The cache is an eight-entry limit, not a byte-size limit. Reading a file is not an
atomic snapshot of concurrent editing, a mapping pinned across an entire run, or
source authentication. Keep the catalogue immutable for a run when cross-call
consistency is required. No historical capture or metadata is rewritten when a
new mapping is loaded. This change does not introduce an external-reference
resolver or contact a provider.

Regression command:

```bash
python -m pytest -q tests/test_source_area_current_contract.py tests/test_source_area_contract.py
```

The new tests exercise the public loader/binding/metadata boundaries with temporary
local catalogues, including same-size/same-mtime replacement, deletion, read denial,
malformed replacement, duplicate members, malformed identities and return isolation.
Full constrained repository CI remains the acceptance gate. No provider request,
credential read, main merge or deployment is part of these tests.
