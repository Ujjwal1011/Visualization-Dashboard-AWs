import streamlit as st
import pandas as pd
import boto3
import time
import os
from dotenv import load_dotenv # Import this

# This line loads the .env file into the environment
load_dotenv() 



# --- CONFIGURATION ---
# Ideally, store these in a .env file or Streamlit secrets (.streamlit/secrets.toml)
# For this example, replace the placeholders below.
AWS_REGION = 'ap-south-1'          # e.g., 'us-east-1'
S3_OUTPUT = 's3://dku-project/Athena Output/'
DATABASE = 'ccdataset'

# --- ATHENA QUERY FUNCTION ---
def run_athena_query(query, database, s3_output):
    """
    Submits a query to Athena and returns the result as a Pandas DataFrame.
    """
    client = boto3.client('athena', region_name=AWS_REGION)
    
    # 1. Submit the query
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    query_execution_id = response['QueryExecutionId']
    
    # 2. Wait for the query to complete
    while True:
        stats = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1) # Wait 1 second before checking again
    
    # 3. Fetch results if successful
    if status == 'SUCCEEDED':
        # Retrieve the result file path from S3 metadata
        result_s3_path = stats['QueryExecution']['ResultConfiguration']['OutputLocation']
        # Read directly into Pandas (smartest way for Streamlit)
        return pd.read_csv(result_s3_path)
    else:
        st.error(f"Query Failed: {stats['QueryExecution']['Status']['StateChangeReason']}")
        return pd.DataFrame()

# --- DASHBOARD LAYOUT ---
st.set_page_config(page_title="Retail Analytics Dashboard", layout="wide")
st.title("📊 Retail Performance Dashboard")
st.markdown("Real-time insights from AWS Athena")

# Create tabs for better organization
tab1, tab2, tab3 = st.tabs(["Overview & Trends", "Geography & Stores", "Customer Insights"])

# --- TAB 1: OVERVIEW & TRENDS ---
with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Daily Revenue Trend")
        q_daily = """
            SELECT date(CAST(timestamp AS TIMESTAMP)) AS day, SUM(net_amount) AS revenue
            FROM ccdataset.transaction
            GROUP BY 1 ORDER BY 1
        """
        df_daily = run_athena_query(q_daily, DATABASE, S3_OUTPUT)
        if not df_daily.empty:
            df_daily['day'] = pd.to_datetime(df_daily['day'])
            st.line_chart(df_daily, x='day', y='revenue')

    with col2:
        st.subheader("Revenue by Payment Method")
        q_payment = """
            SELECT payment_method, SUM(net_amount) AS revenue
            FROM ccdataset.transaction
            GROUP BY payment_method ORDER BY revenue DESC
        """
        df_payment = run_athena_query(q_payment, DATABASE, S3_OUTPUT)
        if not df_payment.empty:
            st.bar_chart(df_payment, x='payment_method', y='revenue')

    # **NEW QUERY 1**: Hourly Sales Trends (Heatmap style logic)
    st.subheader("Peak Shopping Hours (Hourly Trends)")
    q_hourly = """
        SELECT extract(hour from CAST(timestamp AS TIMESTAMP)) as hour_of_day, 
               COUNT(*) as total_transactions
        FROM ccdataset.transaction
        GROUP BY 1 ORDER BY 1
    """
    df_hourly = run_athena_query(q_hourly, DATABASE, S3_OUTPUT)
    if not df_hourly.empty:
        st.bar_chart(df_hourly, x='hour_of_day', y='total_transactions')

# --- TAB 2: GEOGRAPHY & STORES ---
with tab2:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Region")
        # Note: Kept your specific cast fix for store_id
        q_region = """
            SELECT s.region, SUM(t.net_amount) AS revenue
            FROM ccdataset.transaction t
            JOIN ccdataset.stores s ON t.store_id = CAST(s.store_id AS VARCHAR)
            GROUP BY s.region ORDER BY revenue DESC
        """
        df_region = run_athena_query(q_region, DATABASE, S3_OUTPUT)
        if not df_region.empty:
            st.bar_chart(df_region, x='region', y='revenue')

    with col2:
        st.subheader("Customer Distribution by City")
        q_city = """
            SELECT city, COUNT(*) AS customer_count
            FROM ccdataset.customer
            GROUP BY city ORDER BY customer_count DESC LIMIT 10
        """
        df_city = run_athena_query(q_city, DATABASE, S3_OUTPUT)
        if not df_city.empty:
            st.bar_chart(df_city, x='city', y='customer_count')

    st.subheader("Top Performing Stores")
    q_stores = """
        SELECT store_id, SUM(net_amount) AS revenue
        FROM ccdataset.transaction
        GROUP BY store_id ORDER BY revenue DESC LIMIT 15
    """
    df_stores = run_athena_query(q_stores, DATABASE, S3_OUTPUT)
    if not df_stores.empty:
        st.dataframe(df_stores, use_container_width=True)

# --- TAB 3: CUSTOMER INSIGHTS ---
with tab3:
    st.subheader("High Value Customers (Last 30 Days)")
    # Note: Kept your CAST fix for date comparison
    q_top_cust = """
        SELECT customer_id, SUM(net_amount) AS revenue_30d
        FROM ccdataset.transaction
        WHERE CAST(timestamp AS DATE) >= current_date - interval '30' day
        GROUP BY customer_id ORDER BY revenue_30d DESC LIMIT 10
    """
    df_top_cust = run_athena_query(q_top_cust, DATABASE, S3_OUTPUT)
    if not df_top_cust.empty:
        st.table(df_top_cust)

    st.subheader("Customer Lifecycle (RFM Analysis Base)")
    st.markdown("aggregating total spend, order count, and recency for every customer.")
    
    # Note: Kept your DATE_DIFF parameter fix
    q_features = """
        SELECT c.customer_id,
           COUNT(DISTINCT t.transaction_id) AS orders,
           SUM(t.net_amount) AS total_spend,
           AVG(t.net_amount) AS avg_order,
           DATE_DIFF('day', CAST(MAX(t.timestamp) AS DATE), current_date) AS recency_days
        FROM ccdataset.customer c
        LEFT JOIN ccdataset.transaction t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id
        LIMIT 500
    """
    # Adding a limit 500 to prevent browser crashing on large fetches
    df_features = run_athena_query(q_features, DATABASE, S3_OUTPUT)
    
    if not df_features.empty:
        st.dataframe(df_features, use_container_width=True)
        
        # Bonus: Scatter plot for Value vs Frequency
        st.caption("Scatter: Total Spend vs Order Count")
        st.scatter_chart(df_features, x='orders', y='total_spend')