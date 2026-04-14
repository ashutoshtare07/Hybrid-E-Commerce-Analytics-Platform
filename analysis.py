import psycopg2
import pandas as pd

pg = psycopg2.connect(
    host="localhost",
    database="ecommerce_analytics",
    user="postgres",
    password=""
)

print("\n===== TOP 10 PRODUCTS BY CONVERSION RATE =====")
df1 = pd.read_sql("""
    SELECT name, category, total_views, total_orders, conversion_rate
    FROM conversion_summary
    ORDER BY conversion_rate DESC
    LIMIT 10
""", pg)
print(df1.to_string(index=False))

print("\n===== FUNNEL ANALYSIS =====")
df2 = pd.read_sql("""
    SELECT
        SUM(view_count) AS total_views,
        SUM(cart_count) AS total_carts,
        (SELECT COUNT(*) FROM order_items) AS total_purchases,
        ROUND(SUM(cart_count)::decimal / NULLIF(SUM(view_count), 0) * 100, 2) AS view_to_cart_rate,
        ROUND((SELECT COUNT(*) FROM order_items)::decimal / NULLIF(SUM(cart_count), 0) * 100, 2) AS cart_to_purchase_rate
    FROM product_engagement
""", pg)
print(df2.to_string(index=False))

print("\n===== MOST ABANDONED PRODUCTS =====")
df3 = pd.read_sql("""
    SELECT p.name, p.category, p.price,
        COALESCE(SUM(pe.view_count), 0) AS total_views,
        COALESCE(SUM(pe.cart_count), 0) AS total_carts,
        COUNT(DISTINCT oi.order_id) AS total_orders
    FROM products p
    LEFT JOIN product_engagement pe ON p.product_id = pe.product_id
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    GROUP BY p.product_id, p.name, p.category, p.price
    ORDER BY total_orders ASC, total_views DESC
    LIMIT 10
""", pg)
print(df3.to_string(index=False))

print("\n===== TOP 10 SPENDERS =====")
df4 = pd.read_sql("""
    SELECT u.name, COUNT(o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_spent
    FROM users u
    JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.name
    ORDER BY total_spent DESC
    LIMIT 10
""", pg)
print(df4.to_string(index=False))

pg.close()
print("\nDone.")

df1.to_csv("conversion_rate.csv", index=False)
df2.to_csv("funnel_analysis.csv", index=False)
df3.to_csv("abandoned_products.csv", index=False)
df4.to_csv("top_spenders.csv", index=False)
print("\nCSV files exported!")
