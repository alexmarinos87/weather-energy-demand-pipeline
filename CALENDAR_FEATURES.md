# UK-local calendar feature contract

## Purpose

Electricity demand follows human schedules expressed in local civil time, while event identity and leakage boundaries must remain unambiguous. The repository therefore keeps `event_timestamp_utc` as canonical and derives Europe/London calendar features explicitly.

The rule is event_timestamp_utc as canonical identity; local civil time is derived evidence only.

The existing UTC calendar ridge remains the default control. UK-local features are an opt-in comparison contract.

## Derived fields

Every accepted timezone-aware event timestamp produces:

```text
event_timestamp_utc              canonical instant
event_timestamp_local            Europe/London civil time
event_date_local                  local date

hour_of_day_utc                   0..23
day_of_week_utc                   ISO Monday=1 .. Sunday=7
is_weekend_utc                    0 or 1

hour_of_day_local                 0..23
day_of_week_local                 ISO Monday=1 .. Sunday=7
is_weekend_local                  0 or 1
local_utc_offset_minutes          0 for GMT, 60 for BST
is_dst_local                      0 for GMT, 1 for BST
calendar_timezone                 Europe/London
calendar_feature_contract_version uk-local-calendar-v1
```

Timezone-naive values are rejected. They are not silently interpreted as UTC.

## Daylight-saving transitions

The canonical UTC instant prevents ambiguity when Europe/London civil time repeats during the autumn transition.

For example:

```text
2026-10-25T00:30:00Z -> 2026-10-25 01:30 BST, offset 60, is_dst 1
2026-10-25T01:30:00Z -> 2026-10-25 01:30 GMT, offset 0,  is_dst 0
```

Both rows have local hour 1, but remain distinguishable through UTC identity, offset, and DST evidence. This is the repeated local hour contract.

During the spring transition, the local 01:00 hour does not exist. The contract derives civil time from UTC and therefore never invents a nonexistent local timestamp.

## Local model mode

The established feature contract remains:

```text
time-horizon-v1
```

with UTC calendar fields.

Select the local contract explicitly:

```bash
python -m forecasting.run_baseline \
  --demo \
  --calendar-mode uk-local \
  --horizon-minutes 30 60 \
  --output-dir data/forecasting \
  --output-format csv
```

This uses:

```text
time-horizon-uk-calendar-v1
```

and substitutes the UTC hour/day/weekend trio with:

```text
hour_of_day_local
day_of_week_local
is_weekend_local
local_utc_offset_minutes
is_dst_local
```

Demand, lag, rolling, temperature, humidity, weather-age, target, split, label-purge, and evaluation semantics are otherwise unchanged.

Local output names are distinct:

```text
baseline_uk_local_calendar_predictions.csv
baseline_uk_local_calendar_metrics.csv
```

The same `--calendar-mode uk-local` option works with rolling-origin and paired target-weather comparisons.

## Fabric parity

`03_build_gold_tables.py` retains UTC session semantics and writes both UTC and Europe/London calendar fields into `gold_feature_engineering`.

Spark `DAYOFWEEK` uses Sunday=1, so the notebook converts it explicitly to ISO Monday=1 before deriving weekend flags. This aligns Spark output with pandas.

The gold contract is:

```text
data-contracts/gold_features_schema.json
```

## Boundary

UK-local fields are derived model inputs, not source timestamps. UTC continues to own ordering, joins, target matching, training boundaries, rolling origins, deduplication, and audit identity.
