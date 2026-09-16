from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)

from utilities.helpers import get_catalog, landing_path

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("product_id", StringType()),
        StructField("page_url", StringType()),
        StructField("referrer", StringType()),
        StructField("device_type", StringType()),
        StructField("order_id", StringType())
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("snapshot_id", StringType()),
        StructField("snapshot_date", StringType()),
        StructField("product_id", StringType()),
        StructField("sku", StringType()),
        StructField("warehouse_id", StringType()),
        StructField("quantity_on_hand", LongType()),
        StructField("quantity_reserved", LongType()),
        StructField("quantity_available", LongType())
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("sku", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("weight_kg", DoubleType()),
        StructField("price_usd", DoubleType())
    ]
)

ORDERS_ROW_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("sales_channel", StringType()),
        StructField("currency", StringType())
    ]
)

ORDER_ITEMS_ROW_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType()),
        StructField("order_id", StringType()),
        StructField("product_id", StringType()),
        StructField("sku", StringType()),
        StructField("quantity", LongType()),
        StructField("unit_price", DoubleType())
    ]
)

def _read_file_bronze(subfolder: str, schema: StructType, file_format: str):
    """Shared Auto Loader read, with ingestion metadata attached, for one file source."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .schema(schema)
    )
    if file_format == "csv":
        reader = reader.option("header", "true")

    return (
        reader.load(landing_path(subfolder))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

@dp.table(name="bronze_orders",
          comment="Raw storefront clickstream events, append-only, exactly as they arrived.",)
def bronze_orders():
    return _read_file_bronze("orders_cdc", ORDERS_ROW_SCHEMA, "csv")
    

@dp.table(
    name="bronze_order_items",
    comment="Raw storefront clickstream events, append-only, exactly as they arrived.",
)
def bronze_order_items():
    return _read_file_bronze("order_items_cdc", ORDER_ITEMS_ROW_SCHEMA, "csv")

@dp.table(
    name="bronze_products",
    comment="Raw product catalog export, exactly as it arrived.",
)
def bronze_products():
    return _read_file_bronze("products", PRODUCTS_SCHEMA, "csv")


@dp.table(
    name="bronze_clickstream",
    comment="Raw storefront clickstream events, append-only, exactly as they arrived.",
)
def bronze_clickstream():
    return _read_file_bronze("clickstream", CLICKSTREAM_SCHEMA, "json")


@dp.table(
    name="bronze_inventory",
    comment="Raw daily inventory snapshots across all warehouses, exactly as exported.",
)
def bronze_inventory():
    return _read_file_bronze("inventory", INVENTORY_SCHEMA, "csv")
