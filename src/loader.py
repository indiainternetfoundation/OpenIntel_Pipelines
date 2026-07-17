"""
=========================================================
Loader Module
=========================================================

Responsible for writing DataFrames to disk.
"""

import logging

from pyspark.sql import DataFrame

from src.config import (
    ENRICHED_OUTPUT,
    SOA_SUMMARY_OUTPUT,
)

logger = logging.getLogger(__name__)


# =========================================================
# WRITE ENRICHED DATA
# =========================================================

def write_enriched(df: DataFrame):

    logger.info("Writing enriched dataframe...")

    (
        df

        .repartition("source")

        .write

        .mode("append")

        .option("header", True)

        .partitionBy("source")

        .csv(ENRICHED_OUTPUT)
    )

    logger.info("Enriched dataframe written.")


# =========================================================
# WRITE SOA SUMMARY
# =========================================================

def write_soa_summary(df: DataFrame):

    logger.info("Writing SOA summary...")

    (
        df

        .coalesce(1)

        .write

        .mode("append")

        .option("header", True)

        .csv(SOA_SUMMARY_OUTPUT)
    )

    logger.info("SOA Summary written.")