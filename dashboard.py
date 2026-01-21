import os
import pandas as pd
import psycopg2
import streamlit as st

from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=2000, key="refresh")  # 2000ms = 2s


PG_DSN = os.getenv(
    "PG_DSN",
    "dbname=postgres user=postgres password=postgres host=localhost port=5432"
)

st.title("Kafka Demo - Order and Inventory Status")

conn = psycopg2.connect(PG_DSN)

kpi_sql = """
SELECT
COUNT(*) FILTER (WHERE status='RESERVED') AS reserved,
COUNT(*) FILTER (WHERE status='REJECTED') AS rejected,
COUNT(*) AS total
FROM order_status
"""
top_pdn_sql = """
SELECT product_name, COUNT(*) AS cnt
FROM order_status
GROUP BY product_name
ORDER BY cnt DESC
LIMIT 5
"""

latest_sql = """
SELECT order_id, status, product_name, quantity, reason, last_updated_at
FROM order_status
ORDER BY last_updated_at DESC
LIMIT 20
"""

kpi = pd.read_sql(kpi_sql, conn).iloc[0]
st.metric("Total orders", int(kpi["total"]))
st.metric("Reserved", int(kpi["reserved"]))
st.metric("Rejected", int(kpi["rejected"]))

st.subheader("Top Products:")
st.dataframe(pd.read_sql(top_pdn_sql, conn), use_container_width=True)

st.subheader("Latest orders:")
st.dataframe(pd.read_sql(latest_sql, conn), use_container_width=True)