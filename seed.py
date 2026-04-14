import psycopg2
from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

pg = psycopg2.connect(
    host="localhost",
    database="ecommerce_analytics",
    user="postgres",
    password="ashut123"
)
cur = pg.cursor()

mongo = MongoClient("mongodb://localhost:27017")
db = mongo["ecommerce_analytics"]
clickstream = db["clickstream"]

print("Seeding users...")
for _ in range(500):
    cur.execute(
        "INSERT INTO users (name, email, created_at) VALUES (%s, %s, %s)",
        (fake.name(), fake.unique.email(), fake.date_time_this_year())
    )
pg.commit()

print("Seeding products...")
categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
for _ in range(200):
    cur.execute(
        "INSERT INTO products (name, category, price, attributes) VALUES (%s, %s, %s, %s)",
        (fake.catch_phrase(), random.choice(categories), round(random.uniform(10, 5000), 2),
        '{"brand": "' + fake.company() + '", "in_stock": true}')
    )
pg.commit()

print("Seeding orders...")
cur.execute("SELECT user_id FROM users")
user_ids = [r[0] for r in cur.fetchall()]
cur.execute("SELECT product_id, price FROM products")
products = cur.fetchall()

for _ in range(300):
    user_id = random.choice(user_ids)
    order_date = fake.date_time_this_year()
    num_items = random.randint(1, 5)
    selected = random.sample(products, num_items)
    total = sum(p[1] for p in selected)
    cur.execute(
        "INSERT INTO orders (user_id, total_amount, created_at) VALUES (%s, %s, %s) RETURNING order_id",
        (user_id, total, order_date)
    )
    order_id = cur.fetchone()[0]
    for product_id, price in selected:
        qty = random.randint(1, 3)
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, qty, price)
        )
pg.commit()

print("Seeding clickstream...")
cur.execute("SELECT product_id FROM products")
product_ids = [r[0] for r in cur.fetchall()]

events = []
event_types = ["view", "add_to_cart", "search", "scroll"]
for _ in range(10000):
    events.append({
        "user_id": random.choice(user_ids),
        "product_id": random.choice(product_ids),
        "event_type": random.choice(event_types),
        "timestamp": fake.date_time_this_year(),
        "metadata": {
            "scroll_depth": random.randint(10, 100),
            "search_query": fake.bs(),
            "device": random.choice(["mobile", "desktop", "tablet"])
        }
    })
clickstream.insert_many(events)

print("Done! Seeded 500 users, 200 products, 300 orders, 10000 events.")
cur.close()
pg.close()