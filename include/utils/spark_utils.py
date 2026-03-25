import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "brewery_pipeline") -> SparkSession:
    """Returns a SparkSession configured for MinIO (S3A)."""

    jars_dir = os.environ.get("SPARK_JARS_DIR", "/opt/spark/jars")
    jars = ",".join(
        [
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar"),
        ]
    )

    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.jars", jars)
        # ---- S3A / MinIO configs ----
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # ---- write reliability ----
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark