"""Pure Silver transform and DQ rules for MovieLens genome tags."""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


def transform_genome_tags(df):
    """Clean and conform Bronze genome tag data to Silver standards."""
    return (
        df
        .withColumn("tag_id", F.col("tagId").cast(IntegerType()))
        .withColumn("tag_raw", F.col("tag").cast(StringType()))
        .withColumn(
            "tag",
            F.trim(F.regexp_replace(F.regexp_replace(F.col("tag_raw"), r"[\n\t\r]+", " "), r"\s+", " ")),
        )
        .withColumn("tag", F.initcap(F.col("tag")))
        .select("tag_id", "tag", "_ingestion_timestamp")
    )


def get_dq_rules():
    """Return DQ rules that mark malformed genome tag rows for quarantine."""
    return [
        ("NULL_TAG_ID", F.col("tag_id").isNull()),
        ("NULL_TAG", F.col("tag").isNull()),
        ("EMPTY_TAG", F.trim(F.col("tag")) == ""),
    ]
