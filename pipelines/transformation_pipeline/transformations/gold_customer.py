from datetime import timezone, datetime
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr
from transformations.gold_customer_logic import compute_customer_360

@dp.materialized_view(
    name="gold_customer_360",
    comment="Customer Lifetime value, order frequencies, and regional recency parameters for marketing analysis.",
)
def gold_customer_360():
    # Dynamic exchange matrix calculations targeting baseline ZAR currency metrics
    exchange_rates_expr = """
        CASE 
            WHEN currency = 'ZAR' THEN 1.0
            WHEN currency = 'USD' THEN 17.50
            WHEN currency = 'EUR' THEN 19.20
            WHEN currency = 'AUD' THEN 11.80
            ELSE 1.0 
        END
    """
    
    # Filter silver_orders on __END_AT to process active records when calculating sales aggregates
    orders_enriched = (
        spark.read.table("live.silver_order_items")
        .join(spark.read.table("live.silver_orders").filter(col("__END_AT").isNull()), "order_id", "inner")
        .withColumn("exchange_rate", expr(exchange_rates_expr))
        .withColumn("total_zar", (col("quantity") * col("unit_price")) * col("exchange_rate"))
    )
    
    return compute_customer_360(
        customers_df=spark.read.table("live.silver_customers"),
        orders_df=orders_enriched,
        as_of_date=datetime.now(timezone.utc).date(),
    )
