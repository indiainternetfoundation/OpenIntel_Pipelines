"""
=========================================================
OpenIntel ETL
=========================================================

Entry point of the application.
"""

import logging

from src.config import LOG_LEVEL
from src.spark_session import create_spark_session
from src.pipeline import run_pipeline


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


# =========================================================
# MAIN
# =========================================================

def main():

    logger.info("=" * 70)
    logger.info("Starting OpenIntel ETL")
    logger.info("=" * 70)

    spark = None

    try:

        # ----------------------------------------
        # Create Spark Session
        # ----------------------------------------

        spark = create_spark_session()

        logger.info("Spark Session Created Successfully.")

        # ----------------------------------------
        # Run Pipeline
        # ----------------------------------------

        run_pipeline(spark)

        logger.info("Pipeline Completed Successfully.")

    except Exception as e:

        logger.exception(
            "Pipeline Failed : %s",
            str(e)
        )

        raise

    finally:

        if spark is not None:

            logger.info("Stopping Spark Session...")

            spark.stop()

            logger.info("Spark Session Stopped.")

        logger.info("=" * 70)
        logger.info("OpenIntel ETL Finished")
        logger.info("=" * 70)


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    main()