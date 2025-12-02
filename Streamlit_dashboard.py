import streamlit as st
import pandas as pd
import boto3
import time
import os
from dotenv import load_dotenv
# Add this temporary debug code
import os


key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

# --- 1. LOAD ENVIRONMENT VARIABLES ---
# This looks for a .env file to load AWS credentials safely
load_dotenv()

# --- 2. CONFIGURATION ---
# Replace these with your actual details if not in .env
AWS_REGION = 'ap-south-1'           
S3_OUTPUT = 's3://dku-project/Athena Output/'  # Must end with a slash /
DATABASE = 'ccdataset'

# --- 3. ATHENA QUERY FUNCTION ---
def run_athena_query(query, database, s3_output):
    """
    Submits a query to Athena and returns the result as a Pandas DataFrame.
    """
    # Create the Boto3 client using credentials from the environment (.env)
    client = boto3.client(
        'athena', 
        region_name=AWS_REGION
        # Note: boto3 automatically picks up AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from env
    )
    
    # A. Submit the query
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']
    except Exception as e:
        st.error(f"Failed to start query: {e}")
        return pd.DataFrame()
    
    # B. Wait for the query to complete
    while True:
        stats = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1) # Wait 1 second before checking again
    
    # C. Fetch results if successful
    if status == 'SUCCEEDED':
        result_s3_path = stats['QueryExecution']['ResultConfiguration']['OutputLocation']
        # Read directly into Pandas
        return pd.read_csv(result_s3_path)
    else:
        st.error(f"Query Failed: {stats['QueryExecution']['Status']['StateChangeReason']}")
        return pd.DataFrame()

# --- 4. DASHBOARD LAYOUT ---
st.set_page_config(page_title="Retail Analytics Dashboard", layout="wide")
st.title("📊 Retail Performance Dashboard")
st.markdown("Real-time insights from AWS Athena")

# Create tabs
tab1, tab2, tab3 = st.tabs(["Overview & Trends", "Geography & Stores", "Customer Insights"])

# ==========================================
# TAB 1: OVERVIEW & TRENDS
# ==========================================
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
            st.line_chart(df_daily, x='day', y='revenue', color='#FF4B4B')

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


# ==========================================
# TAB 2: GEOGRAPHY & STORES
# ==========================================
with tab2:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Region")
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

    # Top Performing Stores (Visualization only)
    st.subheader("Top Stores by Revenue")
    q_stores = """
        SELECT store_id, SUM(net_amount) AS revenue
        FROM ccdataset.transaction
        GROUP BY store_id ORDER BY revenue DESC LIMIT 15
    """
    df_stores = run_athena_query(q_stores, DATABASE, S3_OUTPUT)
    if not df_stores.empty:
        # Convert store_id to string so it displays as categories, not numbers
        df_stores['store_id'] = df_stores['store_id'].astype(str)
        st.bar_chart(df_stores, x='store_id', y='revenue')

# ==========================================
# TAB 3: CUSTOMER INSIGHTS
# ==========================================
with tab3:
    st.subheader("Top High Value Customers (Last 30 Days)")
    q_top_cust = """
        SELECT customer_id, SUM(net_amount) AS revenue_30d
        FROM ccdataset.transaction
        WHERE CAST(timestamp AS DATE) >= current_date - interval '30' day
        GROUP BY customer_id ORDER BY revenue_30d DESC LIMIT 10
    """
    df_top_cust = run_athena_query(q_top_cust, DATABASE, S3_OUTPUT)
    if not df_top_cust.empty:
        # Visualization instead of Table
        df_top_cust['customer_id'] = df_top_cust['customer_id'].astype(str)
        st.bar_chart(df_top_cust, x='customer_id', y='revenue_30d')

    st.subheader("Customer Lifecycle Scatter: Spend vs Frequency")
    q_features = """
        SELECT c.customer_id,
           COUNT(DISTINCT t.transaction_id) AS orders,
           SUM(t.net_amount) AS total_spend
        FROM ccdataset.customer c
        LEFT JOIN ccdataset.transaction t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id
        LIMIT 500
    """
    df_features = run_athena_query(q_features, DATABASE, S3_OUTPUT)
    
    if not df_features.empty:
        # Scatter chart only
        st.scatter_chart(
            df_features, 
            x='orders', 
            y='total_spend',
            size='total_spend', # Bubble size based on spend
            color='#33ff57'
        )