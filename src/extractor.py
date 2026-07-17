"""
=========================================================
Extractor Module
=========================================================

Responsible for:

1. Building OpenIntel partition paths
2. Reading OpenIntel Parquet files
3. Returning a Spark DataFrame
"""

import logging

from src.config import (
    OI_BUCKET,
    FDNS_BASE,
    PROCESS_DATES,
    TLDS,
)

logger = logging.getLogger(__name__)


# =========================================================
# BUILD PARTITION PATHS
# =========================================================

def build_partition_paths():
    """
    Build all OpenIntel partition paths.

    Returns
    -------
    list
        List of S3A partition paths.
    """

    partition_paths = []

    for tld in TLDS:

        for dt in PROCESS_DATES:

            path = (
                f"s3a://{OI_BUCKET}/"
                f"{FDNS_BASE}/"
                f"basis=zonefile/"
                f"source={tld}/"
                f"year={dt.year}/"
                f"month={dt.month:02d}/"
                f"day={dt.day:02d}"
            )

            partition_paths.append(path)

    logger.info(
        "Discovered %d partition(s).",
        len(partition_paths)
    )

    return partition_paths


# =========================================================
# EXTRACT ONE PARTITION
# =========================================================

def read_partition(
    spark,
    partition_path,
):
    """
    Read one OpenIntel partition.

    Parameters
    ----------
    spark : SparkSession

    partition_path : str

    Returns
    -------
    DataFrame
    """

    logger.info(
        "Reading partition: %s",
        partition_path
    )

    base_path = (
        f"s3a://{OI_BUCKET}/{FDNS_BASE}/"
    )

    df = (
        spark.read
        .option("basePath", base_path)
        .parquet(partition_path)
        .select(
            "query_name",
            "response_type",
            "source",
            "year",
            "month",
            "day",
        )
    )

    return df