import streamlit as st
import psycopg2
import pandas as pd

st.set_page_config(page_title="E-Commerce Analytics", layout="wide")
st.title("🛒 E-Commerce Analytics Dashboard")

pg = psycopg2.connect(
    host="localhost",
    database="ecommerce_analytics",
    user="postgres",
    password="ashut123"
)

# Funnel
st.header("📊 Conversion Funnel")
df_funnel = pd.read_sql("""
    SELECT SUM(view_count) AS Views, SUM(cart_count) AS Carts,
        (SELECT COUNT(*) FROM order_items) AS Purchases
    FROM product_engagement
""", pg)
st.bar_chart(df_funnel.T.rename(columns={0: "Count"}))

# Conversion Rate
st.header("🏆 Top 10 Products by Conversion Rate")
df_conv = pd.read_sql("""
    SELECT name, category, conversion_rate
    FROM conversion_summary
    ORDER BY conversion_rate DESC LIMIT 10
""", pg)
st.dataframe(df_conv, use_container_width=True)

# Abandoned Products
st.header("🚨 Most Abandoned Products")
df_abandon = pd.read_sql("""
    SELECT p.name, p.category, p.price,
        COALESCE(SUM(pe.view_count), 0) AS total_views,
        COALESCE(SUM(pe.cart_count), 0) AS total_carts,
        COUNT(DISTINCT oi.order_id) AS total_orders
    FROM products p
    LEFT JOIN product_engagement pe ON p.product_id = pe.product_id
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    GROUP BY p.product_id, p.name, p.category, p.price
    ORDER BY total_orders ASC, total_views DESC LIMIT 10
""", pg)
st.dataframe(df_abandon, use_container_width=True)

# Top Spenders
st.header("💰 Top 10 Spenders")
df_spend = pd.read_sql("""
    SELECT u.name, COUNT(o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_spent
    FROM users u JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.name ORDER BY total_spent DESC LIMIT 10
""", pg)
st.bar_chart(df_spend.set_index("name")["total_spent"])

pg.close()