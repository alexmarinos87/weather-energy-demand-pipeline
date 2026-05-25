# Fabric notebook source: 02_bronze_to_silver
#
# Reads immutable raw API captures from OneLake Files and rebuilds canonical
# silver Delta tables in the attached Lakehouse.

from pyspark.sql import Window
from pyspark.sql import functions as F


spark.conf.set("spark.sql.session.timeZone", "UTC")

WEATHER_RAW_PATH = "Files/raw/weather/ingestion_date=*/*.json"
ENERGY_RAW_PATH = "Files/raw/energy/ingestion_date=*/*.json"
SILVER_WEATHER_TABLE = "silver_weather"
SILVER_ENERGY_TABLE = "silver_energy"


def _filename_col() -> F.Column:
    return F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)


def _file_timestamp_col(prefix: str) -> F.Column:
    filename = _filename_col()
    timestamp_text = F.regexp_extract(filename, rf"{prefix}_(\d{{8}}_\d{{6}})\.json$", 1)
    return F.to_timestamp(timestamp_text, "yyyyMMdd_HHmmss")


weather_raw = spark.read.option("multiLine", "true").json(WEATHER_RAW_PATH)

weather_event_ts = F.to_timestamp(F.from_unixtime(F.col("dt").cast("long")))
weather_df = (
    weather_raw
    .withColumn("source_file", _filename_col())
    .withColumn("event_timestamp_utc", weather_event_ts)
    .withColumn("ingestion_timestamp_utc", _file_timestamp_col("weather"))
    .select(
        F.lit("weather").alias("source_dataset"),
        F.col("source_file"),
        F.col("id").cast("string").alias("source_record_id"),
        F.col("event_timestamp_utc"),
        F.col("ingestion_timestamp_utc"),
        F.to_date("event_timestamp_utc").alias("event_date_utc"),
        F.col("name").alias("city"),
        F.col("sys.country").alias("country_code"),
        F.col("coord.lat").cast("double").alias("latitude"),
        F.col("coord.lon").cast("double").alias("longitude"),
        F.col("main.temp").cast("double").alias("temperature_c"),
        F.col("main.feels_like").cast("double").alias("feels_like_c"),
        F.col("main.humidity").cast("double").alias("humidity_pct"),
        F.col("main.pressure").cast("double").alias("pressure_hpa"),
        F.col("clouds.all").cast("double").alias("cloud_cover_pct"),
        F.col("wind.speed").cast("double").alias("wind_speed_mps"),
        F.col("weather")[0]["main"].alias("weather_main"),
        F.col("weather")[0]["description"].alias("weather_description"),
    )
)

weather_window = Window.partitionBy("city", "event_timestamp_utc").orderBy(
    F.col("ingestion_timestamp_utc").desc_nulls_last()
)
weather_df = (
    weather_df
    .withColumn("_rn", F.row_number().over(weather_window))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

(
    weather_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date_utc")
    .saveAsTable(SILVER_WEATHER_TABLE)
)


energy_raw = spark.read.option("multiLine", "true").json(ENERGY_RAW_PATH)

energy_df = (
    energy_raw
    .withColumn("source_file", _filename_col())
    .withColumn("ingestion_timestamp_utc", _file_timestamp_col("energy"))
    .withColumn("record", F.explode_outer("result.records"))
    .withColumn("event_timestamp_utc", F.to_timestamp(F.col("record.Timestamp")))
    .select(
        F.lit("energy").alias("source_dataset"),
        F.col("source_file"),
        F.col("result.resource_id").alias("resource_id"),
        F.col("record._id").cast("string").alias("source_record_id"),
        F.col("event_timestamp_utc"),
        F.col("ingestion_timestamp_utc"),
        F.to_date("event_timestamp_utc").alias("event_date_utc"),
        F.col("record.Demand").cast("double").alias("demand_mw"),
        F.col("record.Generation").cast("double").alias("generation_mw"),
        F.col("record.Import").cast("double").alias("import_mw"),
        F.col("record.Solar").cast("double").alias("solar_mw"),
        F.col("record.Wind").cast("double").alias("wind_mw"),
        F.col("record.STOR").cast("double").alias("stor_mw"),
        F.col("record.Other").cast("double").alias("other_mw"),
    )
)

energy_window = Window.partitionBy(
    "resource_id",
    "source_record_id",
    "event_timestamp_utc",
).orderBy(F.col("ingestion_timestamp_utc").desc_nulls_last())

energy_df = (
    energy_df
    .where(F.col("event_timestamp_utc").isNotNull())
    .withColumn("_rn", F.row_number().over(energy_window))
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

(
    energy_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date_utc")
    .saveAsTable(SILVER_ENERGY_TABLE)
)

print(
    {
        "silver_weather_rows": weather_df.count(),
        "silver_energy_rows": energy_df.count(),
    }
)
