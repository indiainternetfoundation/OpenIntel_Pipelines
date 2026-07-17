"""
=========================================================
Aggregator Module
=========================================================

Responsible for generating aggregated datasets from the
transformed OpenIntel dataframe.
"""

import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


# =========================================================
# SOA SUMMARY
# =========================================================

def aggregate_soa(df: DataFrame) -> DataFrame:
    """
    Aggregate SOA records.

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    DataFrame
    """

    logger.info("Creating SOA Summary...")

    soa_df = (

        df

        .filter(
            F.col("response_type") == "SOA"
        )

        .groupBy(
            "source",
            "snapshot_date"
        )

        .agg(
            F.count("*").alias("domain_count")
        )

    )

    logger.info("SOA Summary Created.")

    return soa_df