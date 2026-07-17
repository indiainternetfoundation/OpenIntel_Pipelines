"""
=========================================================
Pipeline Module
=========================================================

Orchestrates the complete ETL pipeline.
"""

import logging

from src.extractor import (
    build_partition_paths,
    read_partition,
)

from src.transformer import transform

from src.aggregator import aggregate_soa

from src.loader import (
    write_enriched,
    write_soa_summary,
)

logger = logging.getLogger(__name__)


# =========================================================
# RUN PIPELINE
# =========================================================

def run_pipeline(spark):

    logger.info("=" * 60)
    logger.info("Starting OpenIntel ETL Pipeline")
    logger.info("=" * 60)

    partition_paths = build_partition_paths()

    total = len(partition_paths)

    for index, partition in enumerate(partition_paths, start=1):

        logger.info(
            "Processing Partition %d/%d",
            index,
            total
        )

        logger.info(partition)

        try:

            # ---------------------------------------
            # Extract
            # ---------------------------------------

            raw_df = read_partition(
                spark,
                partition
            )

            # ---------------------------------------
            # Transform
            # ---------------------------------------

            transformed_df = transform(
                raw_df
            )

            # ---------------------------------------
            # Aggregate
            # ---------------------------------------

            soa_df = aggregate_soa(
                transformed_df
            )

            # ---------------------------------------
            # Load
            # ---------------------------------------

            write_enriched(
                transformed_df
            )

            write_soa_summary(
                soa_df
            )

            logger.info(
                "Finished Partition %d/%d",
                index,
                total
            )

        except Exception as e:

            logger.exception(
                "Failed processing %s",
                partition
            )

            continue

    logger.info("=" * 60)
    logger.info("Pipeline Finished Successfully")
    logger.info("=" * 60)