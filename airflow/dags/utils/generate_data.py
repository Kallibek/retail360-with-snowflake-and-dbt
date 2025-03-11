import random
import uuid
import names
import pandas as pd
from faker import Faker
import numpy as np
from datetime import datetime, timedelta

faker = Faker()

def random_timestamp_last_30_days():
    """Generate a random UTC timestamp within the last 30 days with bias:
       - Weekends (Saturday, Sunday) are twice as likely.
       - Certain hours of the day (e.g. around midday) are more likely.
    """
    now = datetime.utcnow()

    # Build a list of candidate dates from today back 29 days.
    candidate_days = []
    day_weights = []
    for i in range(30):
        day = now - timedelta(days=i)
        # Use only the date portion.
        candidate_days.append(day.date())
        # Weight: weekends get a weight of 2, weekdays 1.
        day_weights.append(2 if day.weekday() >= 5 else 1)

    # Choose a day with weighted probability.
    chosen_day = random.choices(candidate_days, weights=day_weights, k=1)[0]

    # Define hourly weights for the 24 hours. This example gives higher chance
    # to orders during mid-day. You can adjust the values as needed.
    hour_weights = [
        0.5, 0.5, 0.5, 0.5,   # 00-03: low probability
        0.8, 1.0, 1.2, 1.5,   # 04-07: early morning, still low
        2.0, 2.5, 3.0, 3.5,   # 08-11: increasing chance towards morning
        4.0, 4.0, 4.0, 3.5,   # 12-15: peak period around noon/early afternoon
        3.0, 2.5, 2.0, 1.5,   # 16-19: tapering off in the evening
        1.0, 0.8, 0.5, 0.5    # 20-23: low probability at night
    ]
    chosen_hour = random.choices(list(range(24)), weights=hour_weights, k=1)[0]

    # Minutes and seconds chosen uniformly.
    chosen_minute = random.randint(0, 59)
    chosen_second = random.randint(0, 59)

    # Combine the chosen day and time components.
    random_dt = datetime.combine(chosen_day, datetime.min.time()) + timedelta(
        hours=chosen_hour, minutes=chosen_minute, seconds=chosen_second
    )
    return random_dt

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
        # Generate one random timestamp per order.
        timestamp = random_timestamp_last_30_days().isoformat()

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
            "created_at": timestamp
            
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
                "created_at": timestamp
            }
            total_amount += line_total
            data_items.append(item_record)

        # Update total amount in the order record
        order_record["total_amount"] = round(total_amount, 2)

    df_orders = pd.DataFrame(data_orders)
    df_orderitems = pd.DataFrame(data_items)
    return df_orders, df_orderitems
