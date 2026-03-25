import logging
import os
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from include.utils.spark_utils import get_spark_session

logger = logging.getLogger(__name__)


def run_silver(**context: Any) -> None:
    """Reads raw JSON from the bronze layer, applies cleaning
    transformations and writes the result as partitioned Parquet
    to the silver layer.

    Transformations applied:
        - Trims and lowercases string columns.
        - Casts latitude/longitude to double.
        - Drops rows without state_province.
        - Adds ingestion_date column.

    Args:
        **context: Airflow context dict. Uses 'ds' for execution date.
    """
    execution_date: str = context["ds"]
    bucket = os.environ["MINIO_BUCKET"]
    spark = None

    try:
        spark = get_spark_session("brewery_silver")

        bronze_file = f"s3a://{bucket}/bronze/breweries_{execution_date}.json"
        df = spark.read.option("multiline", "true").json(bronze_file)

        # basic data integrity checks
        required_cols = ["id", "name", "brewery_type", "state_province"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in source data: {missing}")

        if df.count() == 0:
            raise ValueError("Bronze file is empty, nothing to process")

        # normalize strings, cast coordinates, drop nulls
        df = (
            df
            .withColumn("brewery_type", F.lower(F.trim(F.col("brewery_type"))))
            .withColumn("state_province", F.trim(F.col("state_province")))
            .withColumn("city", F.trim(F.col("city")))
            .withColumn("country", F.trim(F.col("country")))
            .withColumn("name", F.trim(F.col("name")))
            .withColumn("latitude", F.col("latitude").cast(DoubleType()))
            .withColumn("longitude", F.col("longitude").cast(DoubleType()))
            .withColumn("ingestion_date", F.lit(execution_date))
            .filter(F.col("state_province").isNotNull())
        )

        logger.info("Silver sample (5 rows):")
        df.show(5, truncate=False)
        logger.info("Total records after cleaning: %d", df.count())

        (
            df.write
            .mode("overwrite")
            .partitionBy("state_province")
            .parquet(f"s3a://{bucket}/silver/breweries")
        )

        logger.info("Silver layer done")

    finally:
        if spark:
            spark.stop()