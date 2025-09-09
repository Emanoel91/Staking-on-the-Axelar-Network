import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import networkx as nx

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Staking on the Axelar Network",
    page_icon="https://pbs.twimg.com/profile_images/1877235283755778048/4nlylmxm_400x400.jpg",
    layout="wide"
)

# --- Title -----------------------------------------------------------------------------------------------------
st.title("👨‍🍳Stakers Analysis")

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-09-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))

# --- Functions -----------------------------------------------------------------------------------------------------







@st.cache_data
def load_stakers_overtime(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (select date_trunc('{timeframe}',block_timestamp) as "Date", count(distinct delegator_address) as "Total Stakers"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and action='delegate'
    group by 1
    order by 1),
    table2 as (with tab1 as (select delegator_address, min(block_timestamp::date) as first_tx
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and action='delegate'
    group by 1)
    select date_trunc('{timeframe}',first_tx) as "Date", count(distinct delegator_address) as "New Stakers",
    sum("New Stakers") over (order by "Date") as "Stakers Growth"
    from tab1
    group by 1)
    select table1."Date" as "Date", "Total Stakers", "New Stakers", 
    "Total Stakers"-"New Stakers" as "Returning Stakers", "Stakers Growth"
    from table1 left join table2 on table1."Date"=table2."Date"
    where table1."Date">='{start_str}' AND table1."Date"<='{end_str}'
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_stakers_distribution_count(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (select delegator_address, count(distinct tx_id) as "Staking Count"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
    block_timestamp::date<='{end_str}' and action='delegate'
    group by 1)
    select "Staking Count", count(distinct delegator_address) as "Stakers Count"
    from table1 
    group by 1
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_stakers_distribution_class(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (select delegator_address, count(distinct tx_id), case 
    when count(distinct tx_id)=1 then 'n=1 Txn'
    when count(distinct tx_id)>1 and count(distinct tx_id)<=5 then '1<n<=5 Txns'
    when count(distinct tx_id)>5 and count(distinct tx_id)<=10 then '5<n<=10 Txns'
    when count(distinct tx_id)>10 and count(distinct tx_id)<=20 then '10<n<=20 Txns'
    when count(distinct tx_id)>20 and count(distinct tx_id)<=50 then '20<n<=50 Txns'
    when count(distinct tx_id)>50 and count(distinct tx_id)<=100 then '50<n<=100 Txns'
    else 'n>100 Txns' end as "Class"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
    block_timestamp::date<='{end_str}' and action='delegate'
    group by 1)
    select "Class", count(distinct delegator_address) as "Stakers Count"
    from table1 
    group by 1
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_stakers_distribution_volume(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (select delegator_address, sum(amount)/pow(10,6), case 
    when (sum(amount)/pow(10,6))<=10 then 'V<=10 AXL'
    when (sum(amount)/pow(10,6))>10 and (sum(amount)/pow(10,6))<=100 then '10<V<=100 AXL'
    when (sum(amount)/pow(10,6))>100 and (sum(amount)/pow(10,6))<=1000 then '100<V<=1k AXL'
    when (sum(amount)/pow(10,6))>1000 and (sum(amount)/pow(10,6))<=10000 then '1k<V<=10k AXL'
    when (sum(amount)/pow(10,6))>10000 and (sum(amount)/pow(10,6))<=100000 then '10k<V<=100k AXL'
    when (sum(amount)/pow(10,6))>100000 and (sum(amount)/pow(10,6))<=1000000 then '100k<V<=1M AXL'
    when (sum(amount)/pow(10,6))>1000000 and (sum(amount)/pow(10,6))<=10000000 then '1M<V<=10M AXL'
    else 'V>10M AXL' end as "Class"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
    block_timestamp::date<='{end_str}' and action='delegate'
    group by 1)
    select "Class", count(distinct delegator_address) as "Staker Count"
    from table1 
    group by 1
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_stakers_activity_tracker(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    select delegator_address as "👨‍🍳Staker", count(distinct tx_id) as "🔗Total Staking Count", 
    round((sum(amount)/pow(10,6)),1) as "🥩Total Staking Volume", 
    min(block_timestamp::date) as "📅First Staking Date"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
    block_timestamp::date<='{end_str}' and action='delegate'
    group by 1
    order by 3 desc 
    """

    df = pd.read_sql(query, conn)
    return df
# --- Load Data -----------------------------------------------------------------------------------------------------




df_stakers_overtime = load_stakers_overtime(timeframe, start_date, end_date)
df_stakers_distribution_count = load_stakers_distribution_count(start_date, end_date)
df_stakers_distribution_class = load_stakers_distribution_class(start_date, end_date)
df_stakers_distribution_volume = load_stakers_distribution_volume(start_date, end_date)
df_stakers_activity_tracker = load_stakers_activity_tracker(start_date, end_date)
