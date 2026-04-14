# Hybrid E-Commerce Analytics Platform

## Architecture
- **PostgreSQL** → structured transactional data (users, products, orders)
- **MongoDB** → flexible event/behavioral data (clickstream, views, scrolls)

## Why Hybrid?
- PostgreSQL handles ACID-compliant transactions, joins, and window functions
- MongoDB handles semi-structured JSON logs with flexible schemas
- ETL pipeline bridges both databases into unified analytics

## ADBMS Concepts Used
- **JSONB + GIN Index** → flexible product attributes with fast search
- **ETL Pipeline** → Python script aggregates MongoDB → PostgreSQL
- **Materialized View** → pre-computed conversion rates per product
- **Window Functions** → time-to-purchase analysis per user
- **Funnel Analysis** → views → carts → purchases drop-off rates

## Results
- 500 users, 200 products, 300 orders, 10,000 clickstream events
- Funnel: 96.28% view-to-cart rate, 36.35% cart-to-purchase rate
- Top conversion rate: 20% on select Electronics and Clothing products

## Files
- `seed.py` → seeds PostgreSQL + MongoDB with fake data
- `etl.py` → ETL from MongoDB to PostgreSQL
- `analysis.py` → runs all 4 analyses, exports CSVs
- `dashboard.py` → Streamlit dashboard
- `schema.sql` → PostgreSQL schema
