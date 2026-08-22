# Fabric notebook source: 02b_forecast_weather_to_silver
# Optional/manual forecast-weather evidence path. Attach weather_energy_lakehouse.

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

spark.conf.set("spark.sql.session.timeZone", "UTC")

FORECAST_WEATHER_RAW_PATH = "Files/raw/forecast_weather/ingestion_date=*/*.json"
SILVER_FORECAST_WEATHER_TABLE = "silver_forecast_weather"
PROVIDER = "openweather"
MODEL = "5-day-3-hour"
ISSUE_BASIS = "retrieval_time_surrogate"


def _filename_col() -> F.Column:
    return F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)


def _metadata(dataframe, field: str, data_type: str = "string") -> F.Column:
    if "_pipeline_metadata" not in dataframe.columns:
        return F.lit(None).cast(data_type)
    metadata_type = dataframe.schema["_pipeline_metadata"].dataType
    if not isinstance(metadata_type, StructType) or field not in metadata_type.fieldNames():
        return F.lit(None).cast(data_type)
    return F.col(f"_pipeline_metadata.{field}").cast(data_type)


raw = spark.read.option("multiLine", "true").json(FORECAST_WEATHER_RAW_PATH)
retrieved_at = F.to_timestamp(_metadata(raw, "retrieved_at_utc"))
city_identity = F.regexp_replace(_metadata(raw, "weather_proxy_city"), r",[^,]+$", "")

forecast = (
    raw
    .withColumn("source_file", _filename_col())
    .withColumn("provider_record", F.explode_outer("list"))
    .withColumn("forecast_issued_at_utc", retrieved_at)
    .withColumn("forecast_ingested_at_utc", retrieved_at)
    .withColumn("forecast_retrieved_at_utc", retrieved_at)
    .withColumn(
        "forecast_valid_at_utc",
        F.to_timestamp(F.from_unixtime(F.col("provider_record.dt").cast("long"))),
    )
    .select(
        F.lit("forecast_weather").alias("source_dataset"),
        F.col("source_file"),
        _metadata(raw, "source_area").alias("source_area"),
        _metadata(raw, "source_area_name").alias("source_area_name"),
        _metadata(raw, "contract_version").alias("metadata_contract_version"),
        city_identity.alias("city"),
        F.col("city.country").alias("country_code"),
        F.col("forecast_issued_at_utc"),
        F.col("forecast_ingested_at_utc"),
        F.col("forecast_retrieved_at_utc"),
        F.col("forecast_valid_at_utc"),
        F.to_date("forecast_valid_at_utc").alias("forecast_valid_date_utc"),
        F.col("provider_record.main.temp").cast("double").alias("forecast_temperature_c"),
        F.col("provider_record.main.humidity").cast("double").alias("forecast_humidity_pct"),
        _metadata(raw, "provider").alias("forecast_provider"),
        _metadata(raw, "forecast_model").alias("forecast_model"),
        _metadata(raw, "forecast_issue_basis").alias("forecast_issue_basis"),
        F.col("provider_record.dt").cast("string").alias("forecast_provider_record_id"),
        F.col("city.name").alias("forecast_provider_location_name"),
        F.col("city.coord.lat").cast("double").alias("forecast_latitude"),
        F.col("city.coord.lon").cast("double").alias("forecast_longitude"),
        _metadata(raw, "raw_snapshot_id").alias("raw_snapshot_id"),
    )
)

required = [
    "source_area", "city", "forecast_issued_at_utc", "forecast_ingested_at_utc",
    "forecast_retrieved_at_utc", "forecast_valid_at_utc", "forecast_temperature_c",
    "forecast_humidity_pct", "forecast_provider", "forecast_model",
    "forecast_issue_basis", "forecast_provider_record_id", "raw_snapshot_id",
]
invalid = forecast.where(
    F.col("source_area").isNull()
    | F.col("city").isNull()
    | F.col("forecast_issued_at_utc").isNull()
    | F.col("forecast_valid_at_utc").isNull()
    | (F.col("forecast_issued_at_utc") != F.col("forecast_ingested_at_utc"))
    | (F.col("forecast_ingested_at_utc") != F.col("forecast_retrieved_at_utc"))
    | (F.col("forecast_valid_at_utc") <= F.col("forecast_ingested_at_utc"))
    | (F.col("forecast_humidity_pct") < 0)
    | (F.col("forecast_humidity_pct") > 100)
    | (F.col("forecast_provider") != F.lit(PROVIDER))
    | (F.col("forecast_model") != F.lit(MODEL))
    | (F.col("forecast_issue_basis") != F.lit(ISSUE_BASIS))
    | (F.col("country_code") != F.lit("GB"))
    | (~F.col("raw_snapshot_id").rlike("^[0-9a-f]{64}$"))
).limit(1).count()
if invalid:
    raise ValueError("Forecast-weather bronze data violates the normalized silver contract.")

window = Window.partitionBy(
    "source_area", "city", "forecast_provider", "forecast_model",
    "forecast_issued_at_utc", "forecast_valid_at_utc",
).orderBy(
    F.col("forecast_ingested_at_utc").desc(),
    F.col("raw_snapshot_id").desc(),
    F.col("source_file").desc(),
)
forecast = (
    forecast.dropna(subset=required)
    .withColumn("_rn", F.row_number().over(window))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

if forecast.limit(1).count() == 0:
    raise ValueError("No valid future forecast-weather rows were produced.")

(
    forecast.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("forecast_valid_date_utc")
    .saveAsTable(SILVER_FORECAST_WEATHER_TABLE)
)

print({
    "silver_forecast_weather_rows": forecast.count(),
    "table": SILVER_FORECAST_WEATHER_TABLE,
})
