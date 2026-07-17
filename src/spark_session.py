"""
=========================================================
Spark Session
=========================================================

This module is responsible for creating and configuring
the SparkSession used throughout the ETL pipeline.
"""

import time

from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.config import (
    APP_NAME,
    SPARK_MASTER,
    DRIVER_MEMORY,
    EXECUTOR_MEMORY,
    SHUFFLE_PARTITIONS,
    OI_ENDPOINT,
    ENABLE_VECTORIZED_PARQUET,
    ENABLE_S3_VECTORED_READ,
)

# =========================================================
# CREATE SPARK SESSION
# =========================================================

def create_spark_session():
    """
    Create and return a configured SparkSession.
    """

    conf = SparkConf()

    # -----------------------------------------------------
    # Application
    # -----------------------------------------------------

    conf.setMaster(SPARK_MASTER)

    conf.setAppName(
        f"{APP_NAME}-{int(time.time())}"
    )

    # -----------------------------------------------------
    # Memory
    # -----------------------------------------------------

    conf.set(
        "spark.driver.memory",
        DRIVER_MEMORY
    )

    conf.set(
        "spark.executor.memory",
        EXECUTOR_MEMORY
    )

    # -----------------------------------------------------
    # Shuffle
    # -----------------------------------------------------

    conf.set(
        "spark.sql.shuffle.partitions",
        SHUFFLE_PARTITIONS
    )

    # -----------------------------------------------------
    # Hadoop AWS Packages
    # -----------------------------------------------------

    conf.set(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-aws:3.4.2",
            "software.amazon.awssdk:bundle:2.29.52",
        ])
    )

    # -----------------------------------------------------
    # S3A Configuration
    # -----------------------------------------------------

    conf.set(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    conf.set(
        "spark.hadoop.fs.s3a.endpoint",
        OI_ENDPOINT
    )

    conf.set(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )

    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )

    # -----------------------------------------------------
    # Performance
    # -----------------------------------------------------

    conf.set(
        "spark.sql.parquet.enableVectorizedReader",
        str(ENABLE_VECTORIZED_PARQUET).lower()
    )

    conf.set(
        "spark.hadoop.fs.s3a.vectored.read.enabled",
        str(ENABLE_S3_VECTORED_READ).lower()
    )

    conf.set(
        "spark.sql.adaptive.enabled",
        "true"
    )

    conf.set(
        "spark.sql.files.ignoreCorruptFiles",
        "true"
    )

    # -----------------------------------------------------
    # Create Spark Session
    # -----------------------------------------------------

    spark = (
        SparkSession.builder
        .config(conf=conf)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark