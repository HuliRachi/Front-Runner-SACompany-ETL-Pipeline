from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, count, max as _max, datediff, lit, to_date

def compute_customer_360(
    customers_df: DataFrame,
    orders_df: DataFrame,
    as_of_date: date,
) -> DataFrame:
    """
    BUSINESS QUESTIONS ANSWERED:
    1. Marketing: Who are our top-spending customers, and what is their historical lifetime value?
    2. CRM Strategy: How many total separate orders has each customer placed through our storefronts?
    3. Retention: How many days has it been since a customer last purchased an item from us?
    """
    # Filter out historical row updates; isolate currently active customer records
    customers_current = customers_df.filter(col("__END_AT").isNull())

    # Aggregate total spending value based on the conformed orders input tracking vector
    order_summary = (
        orders_df
        .groupBy("customer_id")
        .agg(
            _sum("total_zar").alias("lifetime_value_zar"),
            count("order_id").alias("order_count"),
            _max("timestamp").alias("last_order_date")
        )
    )

    return (
        customers_current
        .join(order_summary, "customer_id", "left")
        .withColumn(
            "days_since_last_order",
            datediff(lit(as_of_date), to_date(col("last_order_date")))
        )
        .select(
            "customer_id", 
            "first_name", 
            "last_name", 
            "loyalty_tier", 
            "country",
            "lifetime_value_zar", 
            "order_count", 
            "last_order_date", 
            "days_since_last_order"
        )
    )
