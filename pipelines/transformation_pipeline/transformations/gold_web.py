from pyspark import pipelines as dp
from transformations.gold_web_logic import compute_web_conversions

@dp.materialized_view(
    name="gold_web_conversions",
    comment="E-Commerce conversion percentages grouped by browser device type definitions.",
)
def gold_web_conversions():
    return compute_web_conversions(
        clickstream_df=spark.read.table("live.silver_clickstream")
    )