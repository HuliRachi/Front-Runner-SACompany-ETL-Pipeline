from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def compute_web_conversions(
    clickstream_df: DataFrame,
) -> DataFrame:
    """
    BUSINESS QUESTIONS ANSWERED:
    1. Product Team: How much overall client traffic (total click events) does our online storefront experience?
    2. Sales Conversion: Out of all browsing interactions, how many resulted in successful purchase completion checkpoints?
    3. UI/UX Optimization: What is the exact conversion rate percentage comparing mobile, desktop, and tablet configurations?
    """
    return (
        clickstream_df
        .groupBy("device_type")
        .agg(
            F.count("event_id").alias("total_click_events"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("successful_purchases")
        )
        .withColumn(
            "conversion_rate_percentage",
            F.round((F.col("successful_purchases") * 100.0) / F.col("total_click_events"), 2)
        )
        .select(
            "device_type", 
            "total_click_events", 
            "successful_purchases", 
            "conversion_rate_percentage"
        )
    )



