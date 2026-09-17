
from pyspark import pipelines as dp
from transformations.gold_product_logic import compute_product_performance

@dp.materialized_view(
    name="gold_product_performance",
    comment="Top sellers, product catalog statistics, and global stockout alerts for merchandising logistics.",
)
def gold_product_performance():
    return compute_product_performance(
        order_items_df=spark.read.table("live.silver_order_items"),
        products_df=spark.read.table("live.silver_products"),
        inventory_df=spark.read.table("live.silver_inventory"),
        stockout_threshold=25
    )
