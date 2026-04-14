import psycopg2
from pymongo import MongoClient

# Connections
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

# Create product_engagement table in PostgreSQL
cur.execute("""
    CREATE TABLE IF NOT EXISTS product_engagement (
        user_id INT,
        product_id INT,
        view_count INT DEFAULT 0,
        cart_count INT DEFAULT 0,
        search_count INT DEFAULT 0,
        scroll_count INT DEFAULT 0,
        last_seen TIMESTAMP,
        PRIMARY KEY (user_id, product_id)
    )
""")
pg.commit()

# Aggregate from MongoDB
print("Aggregating from MongoDB...")
pipeline = [
    {
        "$group": {
            "_id": {
                "user_id": "$user_id",
                "product_id": "$product_id",
                "event_type": "$event_type"
            },
            "count": {"$sum": 1},
            "last_seen": {"$max": "$timestamp"}
        }
    }
]

results = list(clickstream.aggregate(pipeline))

# Organize into a dict
engagement = {}
for r in results:
    key = (r["_id"]["user_id"], r["_id"]["product_id"])
    event = r["_id"]["event_type"]
    if key not in engagement:
        engagement[key] = {
            "view_count": 0,
            "cart_count": 0,
            "search_count": 0,
            "scroll_count": 0,
            "last_seen": r["last_seen"]
        }
    if event == "view":
        engagement[key]["view_count"] = r["count"]
    elif event == "add_to_cart":
        engagement[key]["cart_count"] = r["count"]
    elif event == "search":
        engagement[key]["search_count"] = r["count"]
    elif event == "scroll":
        engagement[key]["scroll_count"] = r["count"]
    if r["last_seen"] > engagement[key]["last_seen"]:
        engagement[key]["last_seen"] = r["last_seen"]

# Push to PostgreSQL
print("Pushing to PostgreSQL...")
for (user_id, product_id), data in engagement.items():
    cur.execute("""
        INSERT INTO product_engagement 
            (user_id, product_id, view_count, cart_count, search_count, scroll_count, last_seen)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id, product_id) DO UPDATE SET
            view_count = EXCLUDED.view_count,
            cart_count = EXCLUDED.cart_count,
            search_count = EXCLUDED.search_count,
            scroll_count = EXCLUDED.scroll_count,
            last_seen = EXCLUDED.last_seen
    """, (user_id, product_id, data["view_count"], data["cart_count"],
          data["search_count"], data["scroll_count"], data["last_seen"]))

pg.commit()
print("ETL complete! product_engagement table populated.")
cur.close()
pg.close()