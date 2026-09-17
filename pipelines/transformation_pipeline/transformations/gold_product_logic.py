from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, sum as _sum, row_number, desc, coalesce, lit

def compute_product_performance(
    order_items_df: DataFrame,
    products_df: DataFrame,
    inventory_df: DataFrame,
    stockout_threshold: int = 25,
) -> DataFrame:
    """
    BUSINESS QUESTIONS ANSWERED:
    1. Merchandising: Which specific products are our top-selling items by revenue and units sold?
    2. Supply Chain: What is the current aggregate stock level for each product across all active warehouses combined?
    3. Logistics Risk: Which products have items currently sitting below dangerous buffer levels, risking a stockout?
    """
    # FIX: Explicitly multiply quantity by unit_price to get accurate line totals
    sales = (
        order_items_df
        .withColumn("line_total_usd", col("quantity") * col("unit_price"))
        .groupBy("product_id")
        .agg(
            _sum("quantity").alias("units_sold"),
            _sum("line_total_usd").alias("revenue_usd")
        )
    )

    # 2. Get the absolute latest snapshot row entry for each warehouse location
    window_spec = Window.partitionBy("product_id", "warehouse_id").orderBy(desc("snapshot_date"))
    latest_per_warehouse = (
        inventory_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
    )
    
    # 3. Add up current available stock quantities globally
    current_stock = (
        latest_per_warehouse
        .groupBy("product_id")
        .agg(_sum("quantity_available").alias("current_stock"))
    )

    # 4. Join components back to master product rows and evaluate risk parameters
    return (
        products_df
        .join(sales, "product_id", "left")
        .join(current_stock, "product_id", "left")
        .withColumn("units_sold", coalesce(col("units_sold"), lit(0)))
        .withColumn("revenue_usd", coalesce(col("revenue_usd"), lit(0.0)))
        .withColumn("current_stock", coalesce(col("current_stock"), lit(0)))
        .withColumn("stockout_risk", col("current_stock") < stockout_threshold)
        .select(
            "product_id", 
            "sku", 
            "name", 
            "category", 
            "units_sold", 
            "revenue_usd", 
            "current_stock", 
            "stockout_risk"
        )
    )
