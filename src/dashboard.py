
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛒",
    layout="wide"
)
# Automatically refresh dashboard every 10 seconds
st_autorefresh(
    interval=10_000,
    key="ecommerce_dashboard_refresh"
)
st.title("🛒 Real-Time E-Commerce Analytics")
st.caption("Kafka | PostgreSQL | AWS S3 | dbt")
st.info("Portfolio demonstration using synthetic e-commerce events.")

# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_analytics",
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD", "")
}


@st.cache_data(ttl=10)
def load_data():

    connection = psycopg2.connect(**DB_CONFIG)

    try:
        product_data = pd.read_sql_query(
            """
            SELECT *
            FROM analytics.fct_product_events
            ORDER BY total_events DESC
            """,
            connection
        )

        event_data = pd.read_sql_query(
            """
            SELECT
                event_id,
                event_type,
                customer_id,
                order_id,
                product_id,
                product_name,
                product_price,
                event_timestamp
            FROM analytics.stg_events
            ORDER BY event_timestamp DESC
            LIMIT 5000
            """,
            connection
        )

        metrics = pd.read_sql_query(
            """
            SELECT
                COUNT(*) AS total_events,
                COUNT(DISTINCT customer_id) AS unique_customers,
                COUNT(DISTINCT order_id) AS unique_orders,
                COUNT(*) FILTER (
                    WHERE event_type = 'payment_success'
                ) AS successful_payments,
                COUNT(*) FILTER (
                    WHERE event_type = 'payment_failed'
                ) AS failed_payments
            FROM analytics.stg_events
            """,
            connection
        )

        return product_data, event_data, metrics.iloc[0]

    finally:
        connection.close()


# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

try:
    products, events, metrics = load_data()

except Exception as error:
    st.error(f"Database connection failed: {error}")
    st.info(
        "Check that PostgreSQL is running and "
        "POSTGRES_PASSWORD is configured."
    )
    st.stop()


# --------------------------------------------------
# KPI CARDS
# --------------------------------------------------

st.subheader("Business Overview")

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    "Total Events",
    f"{int(metrics['total_events']):,}"
)

col2.metric(
    "Unique Customers",
    f"{int(metrics['unique_customers']):,}"
)

col3.metric(
    "Unique Orders",
    f"{int(metrics['unique_orders']):,}"
)

col4.metric(
    "Successful Payments",
    f"{int(metrics['successful_payments']):,}"
)

col5.metric(
    "Failed Payments",
    f"{int(metrics['failed_payments']):,}"
)


# --------------------------------------------------
# PAYMENT ANALYSIS
# --------------------------------------------------

st.divider()
st.subheader("Payment Analysis")

payment_data = pd.DataFrame({
    "Payment Status": ["Successful", "Failed"],
    "Count": [
        int(metrics["successful_payments"]),
        int(metrics["failed_payments"])
    ]
})

col1, col2 = st.columns(2)

with col1:

    fig = px.pie(
        payment_data,
        names="Payment Status",
        values="Count",
        title="Payment Success vs Failure",
        hole=0.55
    )

    st.plotly_chart(fig, width="stretch")

with col2:

    payment_chart = px.bar(
        payment_data,
        x="Payment Status",
        y="Count",
        color="Payment Status",
        title="Payment Event Counts",
        text="Count"
    )

    st.plotly_chart(
        payment_chart,
        width="stretch"
    )


# --------------------------------------------------
# PRODUCT PERFORMANCE
# --------------------------------------------------

st.divider()
st.subheader("Product Performance")

col1, col2 = st.columns(2)

with col1:

    fig = px.bar(
        products,
        x="product_name",
        y="total_events",
        title="Events by Product",
        text="total_events"
    )

    st.plotly_chart(fig, width="stretch")

with col2:

    fig = px.bar(
        products,
        x="product_name",
        y="unique_orders",
        title="Unique Orders by Product",
        text="unique_orders"
    )

    st.plotly_chart(fig, width="stretch")


# --------------------------------------------------
# EVENT TYPE ANALYSIS
# --------------------------------------------------

st.divider()
st.subheader("Event Distribution")

event_counts = (
    events["event_type"]
    .value_counts()
    .reset_index()
)

event_counts.columns = ["Event Type", "Count"]

fig = px.bar(
    event_counts,
    x="Event Type",
    y="Count",
    color="Event Type",
    text="Count",
    title="Distribution of E-Commerce Events"
)

st.plotly_chart(fig, width="stretch")


# --------------------------------------------------
# EVENT ACTIVITY OVER TIME
# --------------------------------------------------

st.divider()
st.subheader("Event Activity Over Time")

if not events.empty:

    events["event_timestamp"] = pd.to_datetime(
        events["event_timestamp"]
    )

    events["event_minute"] = (
        events["event_timestamp"].dt.floor("min")
    )

    timeline = (
        events.groupby(
            ["event_minute", "event_type"]
        )
        .size()
        .reset_index(name="event_count")
    )

    fig = px.line(
        timeline,
        x="event_minute",
        y="event_count",
        color="event_type",
        markers=True,
        title="Events per Minute"
    )

    st.plotly_chart(fig, width="stretch")


# --------------------------------------------------
# RECENT EVENTS
# --------------------------------------------------

st.divider()
st.subheader("Recent Events")

st.dataframe(
    events.head(50),
    width="stretch",
    hide_index=True
)


# --------------------------------------------------
# REFRESH
# --------------------------------------------------

st.sidebar.title("Dashboard Controls")

if st.sidebar.button("Refresh Dashboard"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption(
    "Data cache refreshes every 10 seconds."
)

st.sidebar.caption(
    f"Last dashboard load: {datetime.now().strftime('%H:%M:%S')}"
)

st.sidebar.success("PostgreSQL connection successful")
