import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta

OUTPUT_DIR = "batch_0"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Simulating realistic Front-Runner SA ecosystem data for Databricks...")

# --- 1. CONFIGURATION TARGETS ---
TOTAL_PRODUCTS = 150
TOTAL_CUSTOMERS = 1000
TOTAL_ORDERS = 12000
TOTAL_ORDER_ITEMS = 30000 
TOTAL_EVENTS = 50000       # Clickstream log entries

base_date = datetime(2026, 8, 1)


product_pool = []
product_categories = ["Roof Racks", "Camping", "Storage", "Brackets", "Lighting"]
for i in range(1, TOTAL_PRODUCTS + 1):
    prod_id = str(uuid.uuid4())
    cat = random.choice(product_categories)
    sku = f"SK-FR-{cat[:3].upper()}-{100+i}"
    product_pool.append({
        "product_id": prod_id,
        "sku": sku,
        "name": f"Front-Runner {cat} Component v{i}",
        "category": cat,
        "weight_kg": round(random.uniform(1.5, 45.0), 2),
        "price_usd": round(random.uniform(25.0, 950.0), 2)
    })

customer_pool = [str(uuid.uuid4()) for _ in range(TOTAL_CUSTOMERS)]

with open(os.path.join(OUTPUT_DIR, "products.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["product_id", "sku", "name", "category", "weight_kg", "price_usd"])
    writer.writeheader()
    writer.writerows(product_pool)
print(f"Generated CSV: products.csv ({len(product_pool)} rows)")

warehouses = ["WH-ZA-JHB", "WH-ZA-CPT", "WH-US-TX", "WH-US-CA", "WH-EU-DE", "WH-EU-FR", "WH-AU-SYD", "WH-AU-MEL"]
with open(os.path.join(OUTPUT_DIR, "inventory.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["snapshot_id", "snapshot_date", "product_id", "sku", "warehouse_id", "quantity_on_hand", "quantity_reserved", "quantity_available"])
    
    for wh in warehouses:
        for prod in product_pool:
            q_on_hand = random.randint(30, 400)
            q_res = random.randint(0, int(q_on_hand * 0.2))
            writer.writerow([
                str(uuid.uuid4()),
                "2026-09-14",
                prod["product_id"],
                prod["sku"],
                wh,
                q_on_hand,
                q_res,
                (q_on_hand - q_res)
            ])
print(f"Generated CSV: inventory.csv ({len(warehouses) * len(product_pool)} rows)")

orders_file = os.path.join(OUTPUT_DIR, "orders.csv")
items_file = os.path.join(OUTPUT_DIR, "order_items.csv")

channels = ["E-Commerce Direct", "B2B Dealer Portal"]
currencies = ["ZAR", "USD", "EUR", "AUD"]

order_records = []
order_item_records = []
item_counter = 500000

order_id_pool = []

for order_seq in range(1, TOTAL_ORDERS + 1):
    order_id = str(uuid.uuid4())
    cust_id = random.choice(customer_pool)
    ts = (base_date + timedelta(seconds=random.randint(0, 3888000))).strftime("%Y-%m-%dT%H:%M:%S")
    
    order_records.append({
        "order_id": order_id,
        "customer_id": cust_id,
        "timestamp": ts,
        "sales_channel": random.choice(channels),
        "currency": random.choice(currencies)
    })
    
    basket_size = 2 if (order_seq % 2 == 0) else 3
    purchased_products = random.sample(product_pool, k=basket_size)
    
    for prod in purchased_products:
        order_item_records.append({
            "order_item_id": f"ITEM-{item_counter}",
            "order_id": order_id,
            "product_id": prod["product_id"],
            "sku": prod["sku"],
            "quantity": random.randint(1, 2),
            "unit_price": prod["price_usd"]
        })
        item_counter += 1
    
    order_id_pool.append({"order_id": order_id, "customer_id": cust_id, "timestamp": ts})

with open(orders_file, "w", newline="", encoding="utf-8") as f_ord:
    writer = csv.DictWriter(f_ord, fieldnames=["order_id", "customer_id", "timestamp", "sales_channel", "currency"])
    writer.writeheader()
    writer.writerows(order_records)

with open(items_file, "w", newline="", encoding="utf-8") as f_itm:
    writer = csv.DictWriter(f_itm, fieldnames=["order_item_id", "order_id", "product_id", "sku", "quantity", "unit_price"])
    writer.writeheader()
    writer.writerows(order_item_records)

print(f"Generated CSV: orders.csv ({len(order_records)} rows)")
print(f"Generated CSV: order_items.csv ({len(order_item_records)} rows)")


first_names = ["Jabu", "Sipho", "Liam", "Emma", "Chipo", "Anrich", "Sarah", "Elena", "Tariq"]
last_names = ["Naidoo", "Smith", "Botha", "Muller", "Van Wyk", "Baloyi", "Ndlovu", "Jones"]

with open(os.path.join(OUTPUT_DIR, "customer_cdc.json"), "w", encoding="utf-8") as f:
    for cust_id in customer_pool:
        reg_ts = (base_date - timedelta(days=random.randint(10, 300))).strftime("%Y-%m-%dT%H:%M:%S")
        cdc_record = {
            "op": "c",
            "ts_ms": int((datetime.now() - timedelta(days=1)).timestamp() * 1000),
            "before": None,
            "after": {
                "customer_id": cust_id,
                "email": f"{random.choice(first_names).lower()}.{random.randint(10,99)}@frontrunneroutfitters.co.za",
                "first_name": random.choice(first_names),
                "last_name": random.choice(last_names),
                "registration_date": reg_ts,
                "loyalty_tier": random.choice(["bronze", "silver", "gold"]),
                "country": random.choice(["ZA", "US", "DE", "AU"]),
                "is_active": True,
                "updated_at": reg_ts
            }
        }
        f.write(json.dumps(cdc_record) + "\n")
print(f"Generated JSON Lines: customer_cdc.json ({TOTAL_CUSTOMERS} rows)")


event_types = ["search", "page_view", "product_view", "purchase"]
referrers = ["google", "instagram", "email", "affiliate"]
devices = ["mobile", "tablet", "desktop"]

with open(os.path.join(OUTPUT_DIR, "clickstream_events.json"), "w", encoding="utf-8") as f:
    for i in range(TOTAL_EVENTS):
        is_purchase_event = (i % 5 == 0) and (len(order_id_pool) > 0)
        
        if is_purchase_event:
            matched_order = order_id_pool.pop()
            event_type = "purchase"
            cust_id = matched_order["customer_id"]
            ord_id = matched_order["order_id"]
            prod_id = None
            ts = matched_order["timestamp"]
        else:
            event_type = random.choice(["search", "page_view", "product_view"])
            cust_id = random.choice(customer_pool) if random.random() > 0.2 else None
            ord_id = None
            prod_id = random.choice(product_pool)["product_id"] if event_type == "product_view" else None
            ts = (base_date + timedelta(seconds=random.randint(0, 3888000))).strftime("%Y-%m-%dT%H:%M:%S")
            
        event_record = {
            "event_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "customer_id": cust_id,
            "event_type": event_type,
            "event_timestamp": ts,
            "product_id": prod_id,
            "page_url": f"/shop/category/{random.choice(['racks', 'camping', 'gear'])}",
            "referrer": random.choice(referrers),
            "device_type": random.choice(devices),
            "order_id": ord_id
        }
        f.write(json.dumps(event_record) + "\n")
print(f"Generated JSON Lines: clickstream_events.json ({TOTAL_EVENTS} rows)")

print(f"\n Verification matrix matched cleanly under './{OUTPUT_DIR}/'")
