import random
import uuid
import names
import pandas as pd
from faker import Faker
import numpy as np
from datetime import datetime

faker = Faker()

def generate_customers(num_customers=1000):
    # Example dimension with basic columns
    data = []
    for _ in range(num_customers):
        customer_id = str(uuid.uuid4())  # or any unique random string
        record = {
            "customer_id": customer_id,
            "first_name": names.get_first_name() if random.random() > 0.01 else None,  # 1% chance of NULL
            "last_name": names.get_last_name() if random.random() > 0.01 else None,
            "email": faker.email(),
            "phone_number": faker.phone_number(),
            "address": faker.street_address(),
            "city": faker.city(),
            "state_province": faker.state_abbr(),
            "country": faker.country(),
            "postal_code": faker.postcode(),
            "created_at": datetime.utcnow().isoformat()
        }
        # Introduce random duplication
        if random.random() < 0.02:  # 2% chance
            data.append(record.copy())  # Duplicate record to test dedup logic

        data.append(record)

    df_customers = pd.DataFrame(data)
    return df_customers

def generate_products(num_products=200):
    categories = ["Electronics", "Apparel", "Home & Kitchen", "Sports", "Books"]
    data = []
    for _ in range(num_products):
        product_id = str(uuid.uuid4())
        category = random.choice(categories)
        price = round(random.uniform(5, 500), 2)
        # Introduce out-of-bound price anomalies
        if random.random() < 0.01:
            price = -abs(price)  # negative price anomaly
        record = {
            "product_id": product_id,
            "product_name": f"{category} - {faker.word()}",
            "category": category,
            "brand": faker.company(),
            "price": price,
            "created_at": datetime.utcnow().isoformat()
        }
        data.append(record)

    df_products = pd.DataFrame(data)
    return df_products

def generate_stores(num_stores=50):
    store_types = ["Online", "Brick and Mortar"]
    data = []
    for i in range(num_stores):
        store_id = f"STORE-{i+1}"
        store_type = random.choice(store_types)
        record = {
            "store_id": store_id,
            "store_name": f"{store_type} {i+1}",
            "store_type": store_type,
            "city": faker.city(),
            "state_province": faker.state_abbr(),
            "country": faker.country(),
            "postal_code": faker.postcode(),
            "created_at": datetime.utcnow().isoformat()
        }
        data.append(record)

    df_stores = pd.DataFrame(data)
    return df_stores

def generate_orders_and_orderitems(df_customers, df_products, df_stores, num_orders=5000):
    data_orders = []
    data_items = []
    for _ in range(num_orders):
        order_id = str(uuid.uuid4())
        customer_row = df_customers.sample(1).iloc[0]
        store_row = df_stores.sample(1).iloc[0]
        # Basic shipping cost and discount
        shipping_cost = round(random.uniform(0, 15), 2)
        discount_amount = round(random.uniform(0, 10), 2)

        order_record = {
            "order_id": order_id,
            "customer_id": customer_row["customer_id"],
            "store_id": store_row["store_id"],
            "order_status": random.choice(["Pending", "Shipped", "Delivered", "Returned"]),
            "total_amount": 0.0,  # Will update later
            "discount_amount": discount_amount,
            "shipping_cost": shipping_cost,
            "payment_type": random.choice(["Credit Card", "PayPal", "Gift Card"]),
            "created_at": datetime.utcnow().isoformat()
            
        }
        data_orders.append(order_record)

        # Create 1-5 line items
        num_items = random.randint(1, 5)
        total_amount = 0.0
        for __ in range(num_items):
            product_row = df_products.sample(1).iloc[0]
            quantity = random.randint(1, 3)
            # Introduce random negative quantity
            if random.random() < 0.01:
                quantity = -quantity
            line_total = round(product_row["price"] * quantity, 2) if product_row["price"] > 0 else 0
            item_record = {
                "order_id": order_id,
                "product_id": product_row["product_id"],
                "quantity": quantity,
                "unit_price": product_row["price"],
                "line_total": line_total,
                "line_discount": round(random.uniform(0, 5), 2),
                "created_at": datetime.utcnow().isoformat()
            }
            total_amount += line_total
            data_items.append(item_record)

        # Update total amount in the order record
        order_record["total_amount"] = round(total_amount, 2)

    df_orders = pd.DataFrame(data_orders)
    df_orderitems = pd.DataFrame(data_items)
    return df_orders, df_orderitems
