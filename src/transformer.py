import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


# =========================================================
# TRANSFORM
# =========================================================

def transform(
    df: DataFrame,
) -> DataFrame:
    """
    Clean and enrich the dataframe.

    Steps
    -----

    1. Lowercase query_name

    2. Remove trailing dot

    3. Create snapshot_date

    Returns
    -------
    DataFrame
    """

    logger.info(
        "Transforming dataframe..."
    )

    df = (

        df

        # lowercase
        .withColumn(
            "query_name",
            F.lower(
                F.col("query_name")
            )
        )

        # remove trailing dot
        .withColumn(
            "query_name",
            F.regexp_replace(
                F.col("query_name"),
                r"\.$",
                ""
            )
        )

        # snapshot date
        .withColumn(
            "snapshot_date",
            F.to_date(
                F.concat_ws(
                    "-",
                    F.col("year"),
                    F.lpad(
                        F.col("month"),
                        2,
                        "0"
                    ),
                    F.lpad(
                        F.col("day"),
                        2,
                        "0"
                    ),
                )
            )
        )

    )

    logger.info(
        "Transformation completed."
    )

    return df