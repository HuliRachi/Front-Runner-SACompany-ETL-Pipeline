from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

from utilities.helpers import bronze_table


# ---------------------------------------------------------------------------
# products — snapshot source, AUTO CDC upsert (SCD Type 1), report-only rule
# ---------------------------------------------------------------------------

PRODUCTS_RULES = {
    "retail_above_cost": "price_usd > weight_kg",
}


@dp.view
def products_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_products_valid"))
    )


dp.create_streaming_table(
    name="silver_products",
    comment="Products, current state only. Upserted by product_id, latest file wins.",
    expect_all=PRODUCTS_RULES,
    schema="""
        product_id STRING NOT NULL,
        sku STRING,
        name STRING,
        category STRING,
        weight_kg DOUBLE,
        price_usd DOUBLE,

        CONSTRAINT pk_silver_products PRIMARY KEY (product_id)
    """,
)

dp.create_auto_cdc_flow(
    target="silver_products",
    source="products_change_feed",
    keys=["product_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
    except_column_list= ["_ingested_at", "_source_file", "is_quarantined"]
)


# ---------------------------------------------------------------------------
# clickstream, inventory — event/timeseries sources, conform + report-only rules
# ---------------------------------------------------------------------------

CLICKSTREAM_RULES = {
    "purchase_has_order": "NOT (event_type = 'purchase' AND order_id IS NULL)",
    "product_event_has_product": "NOT (event_type IN ('product_view', 'add_to_cart') AND product_id IS NULL)",
}


@dp.table(
    comment="Clickstream events, conformed — real timestamp type. Already an append-only "
    "event log, so no merge logic is needed; every row is naturally distinct.",
)
@dp.expect_all(CLICKSTREAM_RULES)
def silver_clickstream():
    return (
        spark.readStream.table(bronze_table("bronze_clickstream_valid"))
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
        .drop("_ingested_at", "_source_file", "is_quarantined")
    )

INVENTORY_RULES = {
    "available_matches": "quantity_available = quantity_on_hand - quantity_reserved",
    "reserved_not_exceeding": "quantity_reserved <= quantity_on_hand",
}


@dp.table(
    comment="Inventory snapshots, conformed — real date type. Each day's snapshot is "
    "distinct from the last, not a replacement for it, so no merge logic is needed.",
)
@dp.expect_all(INVENTORY_RULES)
def silver_inventory():
    return (
        spark.readStream.table(bronze_table("bronze_inventory_valid"))
        .withColumn("snapshot_date", col("snapshot_date").cast("date"))
        .drop("_ingested_at", "_source_file", "is_quarantined")
    )


# ---------------------------------------------------------------------------
# orders — SCD Type 2, report-only business rules
# ---------------------------------------------------------------------------

ORDERS_RULES = {
    "valid_order": "(order_id IS NOT NULL AND customer_id IS NOT NULL) ",
}


@dp.view
def orders_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_orders_valid"))
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
    )


dp.create_streaming_table(
    name="silver_orders",
    comment="Orders, SCD Type 2 — full history of status changes over time.",
    expect_all=ORDERS_RULES,
)

dp.create_auto_cdc_flow(
    target="silver_orders",
    source="orders_change_feed",
    keys=["order_id"],
    sequence_by="timestamp",
    stored_as_scd_type=2,
    ignore_null_updates=True,
)



# ---------------------------------------------------------------------------
# order_items — SCD Type 1, MIXED handling: report-only + one quarantined rule
# ---------------------------------------------------------------------------

ORDER_ITEMS_RULES = {
    "line_total_matches": "ABS(quantity * unit_price) < 0.01",
}

ORDER_ITEMS_QUARANTINE_RULE = "NOT (line_total_matches)"


@dp.view
def order_items_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_order_items_valid"))
    )


dp.create_streaming_table(
    name="silver_order_items_all",
    comment="Order items, unfiltered — every row, good and bad line_total alike. "
    "Not what Gold reads. See silver_order_items for the clean table.",
    expect_all=ORDER_ITEMS_RULES,
)

dp.create_auto_cdc_flow(
    target="silver_order_items_all",
    source="order_items_change_feed",
    keys=["order_item_id"],
    stored_as_scd_type=1,
    sequence_by="sku",
    
)

@dp.table(private=True)
def silver_order_items_quality_check():
    return (
        spark.readStream.table("silver_order_items_all")
        .withColumn("line_total_matches", expr(ORDER_ITEMS_RULES["line_total_matches"]))
        .withColumn("is_quarantined", expr(f"NOT ({ORDER_ITEMS_RULES['line_total_matches']})"))
    )

@dp.table(
    name="silver_order_items",
    comment="Order items, current state, line_total verified. This is the table Gold reads — "
    "plain name, same convention as every other Silver table.",
    schema="""
        order_item_id STRING NOT NULL,
        order_id STRING,
        product_id STRING,
        sku STRING,
        quantity LONG,
        unit_price DOUBLE,
        CONSTRAINT pk_silver_order_items PRIMARY KEY (order_item_id)
    """,
)
def silver_order_items():
    return (
        spark.read.table("silver_order_items_quality_check")
        .filter("is_quarantined = false")
        .drop("line_total_matches", "is_quarantined")
        .drop("_ingested_at", "_source_file", "is_quarantined")
    )


@dp.table(
    name="silver_order_items_quarantined",
    comment="Order items where line_total didn't match quantity * unit_price. "
    "Investigation surface — monitored in L16, never hand-edited (see remediation pattern).",
)
def silver_order_items_quarantined():
    return (
        spark.read.table("silver_order_items_quality_check")
        .filter("is_quarantined = true")
        .drop("line_total_matches", "is_quarantined")
    )


