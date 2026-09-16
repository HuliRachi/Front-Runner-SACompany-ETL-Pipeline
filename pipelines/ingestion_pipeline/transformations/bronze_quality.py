from pyspark import pipelines as dp
from pyspark.sql.functions import expr

from bronze_quality_logic import quarantine_rule

# ---------------------------------------------------------------------------
# bronze_customers (CDC) 
# ---------------------------------------------------------------------------

CUSTOMERS_RULES = {
    "valid_customer_id": "after.customer_id IS NOT NULL",
    "valid_email": "after.email IS NOT NULL",
    "valid_loyalty_tier": "after.loyalty_tier IS NULL OR after.loyalty_tier IN ('bronze', 'silver', 'gold')",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CUSTOMERS_RULES)
def bronze_customers_quality_check():
    return spark.readStream.table("bronze_customers").withColumn(
        "is_quarantined", expr(quarantine_rule(CUSTOMERS_RULES))
    )


@dp.table(comment="Customers that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_customers_valid():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = false")


@dp.table(comment="Customers that failed at least one structural quality check. Published — monitored in L19.")
def bronze_customers_quarantined():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = true")

# ---------------------------------------------------------------------------
# bronze_clickstream (file-based) 
# ---------------------------------------------------------------------------

CLICKSTREAM_RULES = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CLICKSTREAM_RULES)
def bronze_clickstream_quality_check():
    return spark.readStream.table("bronze_clickstream").withColumn(
        "is_quarantined", expr(quarantine_rule(CLICKSTREAM_RULES))
    )


@dp.table(comment="Clickstream events that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_clickstream_valid():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = false")


@dp.table(comment="Clickstream events that failed at least one structural quality check. Published — monitored in L19.")
def bronze_clickstream_quarantined():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = true")

# ---------------------------------------------------------------------------
# bronze_inventory (file-based)
# ---------------------------------------------------------------------------

INVENTORY_RULES = {
    "valid_snapshot_id": "snapshot_id IS NOT NULL",
    "valid_product_ref": "product_id IS NOT NULL",
    "valid_quantity_on_hand": "quantity_on_hand IS NULL OR quantity_on_hand >= 0",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(INVENTORY_RULES)
def bronze_inventory_quality_check():
    return spark.readStream.table("bronze_inventory").withColumn(
        "is_quarantined", expr(quarantine_rule(INVENTORY_RULES))
    )


@dp.table(comment="Inventory snapshots that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_inventory_valid():
    return spark.readStream.table("bronze_inventory_quality_check").filter("is_quarantined = false")


@dp.table(comment="Inventory snapshots that failed at least one structural quality check. Published — monitored in L19.")
def bronze_inventory_quarantined():
    return spark.readStream.table("bronze_inventory_quality_check").filter("is_quarantined = true")

# ---------------------------------------------------------------------------
# bronze_products (file-based) 
# ---------------------------------------------------------------------------

PRODUCTS_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_sku": "sku IS NOT NULL",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(PRODUCTS_RULES)
def bronze_products_quality_check():
    return spark.readStream.table("bronze_products").withColumn(
        "is_quarantined", expr(quarantine_rule(PRODUCTS_RULES))
    )


@dp.table(comment="Products that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_products_valid():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = false")


@dp.table(comment="Products that failed at least one structural quality check. Published — monitored in L19.")
def bronze_products_quarantined():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = true")

# ---------------------------------------------------------------------------
# bronze_orders (File-Based)
# ---------------------------------------------------------------------------

ORDERS_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_ref": "customer_id IS NOT NULL",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(ORDERS_RULES)
def bronze_orders_quality_check():
    return spark.readStream.table("bronze_orders").withColumn(
        "is_quarantined", expr(quarantine_rule(ORDERS_RULES))
    )


@dp.table(comment="Orders that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_orders_valid():
    return spark.readStream.table("bronze_orders_quality_check").filter("is_quarantined = false")


@dp.table(comment="Orders that failed at least one structural quality check. Published — monitored in L19.")
def bronze_orders_quarantined():
    return spark.readStream.table("bronze_orders_quality_check").filter("is_quarantined = true")

# ---------------------------------------------------------------------------
# bronze_order_items (File-Based) 
# ---------------------------------------------------------------------------

ORDER_ITEMS_RULES = {
    "valid_order_item_id": "order_item_id IS NOT NULL",
    "valid_order_ref": "order_id IS NOT NULL",
    "valid_product_ref": "product_id IS NOT NULL",
    "valid_quantity": "quantity IS NULL OR quantity > 0",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(ORDER_ITEMS_RULES)
def bronze_order_items_quality_check():
    return spark.readStream.table("bronze_order_items").withColumn(
        "is_quarantined", expr(quarantine_rule(ORDER_ITEMS_RULES))
    )


@dp.table(comment="Order items that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_order_items_valid():
    return spark.readStream.table("bronze_order_items_quality_check").filter("is_quarantined = false")


@dp.table(comment="Order items that failed at least one structural quality check. Published — monitored in L19.")
def bronze_order_items_quarantined():
    return spark.readStream.table("bronze_order_items_quality_check").filter("is_quarantined = true")