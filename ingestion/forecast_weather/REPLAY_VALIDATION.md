# Retained OpenWeather replay validation

`normalize_openweather_forecast` reuses `_validate_provider_snapshot` before
filtering forecast slots. Reading a saved capture no longer bypasses the fetch
path's checks for count/list agreement, request bounds, unique strictly increasing
valid times, country and proxy-coordinate agreement. Expired slots still undergo
these checks; removing them must not conceal contradictory provider evidence.

Replay uses the proxy city and coordinates retained in `_pipeline_metadata`, not
today's source-area catalogue. This is deliberate: a later catalogue edit must not
silently reinterpret a historical capture. No provider request, credential lookup
or current-binding resolution is performed during normalization.

The retained proxy must have a nonempty, already-trimmed city name followed by a
two-letter ASCII country code. Retained latitude/longitude must be finite and in
their physical ranges; booleans and missing coordinates are rejected. Finite
numeric coordinate strings remain supported. Existing country comparison and
coordinate tolerance are delegated to the provider validator, not duplicated in
a second calculation path.

When present, `requested_record_count` must be an actual JSON integer from 1 to 40;
`returned_record_count` must be an integer equal to the retained list length.
Booleans, floats, strings and nulls are not coerced to integers or treated as
missing. Older captures omitting these optional count fields remain supported:
the existing 40-record provider limit is used when the requested count is absent,
and the provider's own `cnt` must still agree with its list.

Valid normalization output, future-slot filtering, retained retrieval-time issue
and availability timestamps, and snapshot identifiers remain unchanged. Source
objects and saved files are not modified. Invalid evidence raises before the main
entry point invokes either saver. This change does not alter fetch preflight,
provider endpoint configuration, schemas or output-publication functions.

These are internal consistency checks, **not authentication**. The existing
`raw_snapshot_id` digest is not recomputed or verified here. Coordinated changes
to retained binding and provider payload can remain internally consistent. The
validator does not establish original provider identity, authenticate timestamps,
infer missing unit provenance or pin a catalogue across a run. Existing raw and
normalized writer collision/partial-publication limitations remain separate.

Regression command:

```bash
python -m pytest -q tests/test_forecast_replay_validation.py tests/test_openweather_forecast_ingestion.py
```

Fixtures come from the actual fetch adapter with injected dummy credentials and
HTTP responses. Replay then forbids all credential, network and current-binding
calls. Tests cover malformed saved evidence, legacy counts, historical proxy
changes, saved-JSON round trips, source immutability, expired-slot consistency and
no-save behavior. Full constrained repository CI remains the acceptance gate.
