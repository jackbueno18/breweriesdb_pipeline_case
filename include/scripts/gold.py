import logging
import os

from pyspark.sql import functions as F

from include.utils.spark_utils import get_spark_session

logger = logging.getLogger(__name__)


def run_gold(**context) -> None:
    """Aggregates brewery count by type and state from the silver layer."""
    bucket = os.environ["MINIO_BUCKET"]
    spark = None

    try:
        spark = get_spark_session("brewery_gold")

        df = spark.read.parquet(f"s3a://{bucket}/silver/breweries")

        df = (
            df.groupBy("brewery_type", "state_province")
            .agg(F.count("id").alias("qty_breweries"))
            .orderBy("state_province", "brewery_type")
        )

        logger.info("Gold sample (5 rows):")
        df.show(5, truncate=False)
        logger.info("Total aggregated rows: %d", df.count())

        (
            df.write
            .mode("overwrite")
            .parquet(f"s3a://{bucket}/gold/breweries_agg")
        )

        logger.info("Gold layer done!")

    finally:
        if spark:
            spark.stop()