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
# --- Row 1 ---------------------------------------------------------------------------------------------------------
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
    select table1."Date" as "Date", "Total Stakers", "New Stakers", "Total Stakers"-"New Stakers" as "Returning Stakers", "Stakers Growth"
    from table1 left join table2 on table1."Date"=table2."Date"
    where table1."Date">='{start_str}' AND table1."Date"<='{end_str}'
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 1 ---------------------------------------------------------------------------------------------------
df_stakers_overtime = load_stakers_overtime(timeframe, start_date, end_date)
# --- Charts: Row 1 ------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig_b1 = go.Figure()
    # Stacked Bars
    fig_b1.add_trace(go.Bar(x=df_stakers_overtime["Date"], y=df_stakers_overtime["New Stakers"], name="New Stakers", marker_color="#0ed145"))
    fig_b1.add_trace(go.Bar(x=df_stakers_overtime["Date"], y=df_stakers_overtime["Returning Stakers"], name="Returning Stakers", marker_color="blue"))
    fig_b1.add_trace(go.Scatter(x=df_stakers_overtime["Date"], y=df_stakers_overtime["Total Stakers"], name="Total Stakers", mode="lines", line=dict(color="black", width=2)))
    fig_b1.update_layout(barmode="stack", title="Number of Stakers Over Time", yaxis=dict(title="Wallet count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    st.plotly_chart(fig_b1, use_container_width=True)

with col2:
    fig2 = px.area(df_stakers_overtime, x="Date", y="Stakers Growth", title="Stakers Growth Over Time")
    fig2.update_layout(xaxis_title="", yaxis_title="wallet count", template="plotly_white")
    st.plotly_chart(fig2, use_container_width=True)

# --- Row 2 ----------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_stakers_by_quarter():
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with date_start as (
    with dates AS (
    SELECT CAST('2022-02-10' AS DATE) AS start_date 
    UNION ALL
    SELECT DATEADD(day, 1, start_date)
    FROM dates
    WHERE start_date < CURRENT_DATE())
    SELECT date_trunc(day, start_date) AS start_date
    FROM dates),
    axl_stakers_balance as (
    select * from
        (select user, sum(amount)/1e6 as balance, min(block_timestamp) as join_date
        from (
            select block_timestamp, DELEGATOR_ADDRESS as user, -1* amount as amount, TX_ID as tx_hash
            from axelar.gov.fact_staking
            where action='undelegate' and TX_SUCCEEDED=TRUE
            union all 
            select block_timestamp, DELEGATOR_ADDRESS, amount, TX_ID
            from axelar.gov.fact_staking
            where action='delegate' and TX_SUCCEEDED=TRUE)
        group by 1)
    where balance>=0.001 and balance is not null),
    axl_stakers_reward as (
    select DELEGATOR_ADDRESS as user, sum(amount)/1e6 as reward
    from axelar.gov.fact_staking_rewards
    group by 1),
    top_stakers as (
    select a.user, balance, reward, join_date 
    from axl_stakers_balance a 
    left join axl_stakers_reward b
    on a.user=b.user
    order by 2 desc
    )

    select year(join_date)||'-Q'||CEIL(month(join_date)/3) as "Year", count(*) as "Stakers"
    from top_stakers 
    group by 1
    order by 1
    """
    
    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 2 -----------------------------------------------------------------------------------------------------
df_stakers_by_quarter = load_stakers_by_quarter()
# --- Chart: Row 2 ---------------------------------------------------------------------------------------------------------
fig_b1 = go.Figure()
fig_b1.add_trace(go.Bar(x=df_stakers_by_quarter["Year"], y=df_stakers_by_quarter["Stakers"], name="Number of Stakers"))
fig_b1.update_layout(barmode="stack", title="Stakers Join Date by Quarter", yaxis=dict(title="Wallet count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
st.plotly_chart(fig_b1, use_container_width=True)

# --- Row 3 ----------------------------------------------------------------------------------------------------------------
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
    order by 2 desc 
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 3 -------------------------------------------------------------------------------------------------
df_stakers_distribution_class = load_stakers_distribution_class(start_date, end_date)
# --- Chart: Row 3 -----------------------------------------------------------------------------------------------------
bar_fig = px.bar(df_stakers_distribution_class, x="Class", y="Stakers Count", title="Breakdown of Stakers by Staked Volume", color_discrete_sequence=["blue"])
bar_fig.update_layout(xaxis_title="", yaxis_title="wallet count", bargap=0.2)

fig_donut_volume = px.pie(df_stakers_distribution_class, names="Class", values="Stakers Count", title="Share of Stakers by Staked Volume", hole=0.5, color="Stakers Count")
fig_donut_volume.update_traces(textposition='inside', textinfo='percent', pull=[0.05]*len(df_stakers_distribution_class))
fig_donut_volume.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(bar_fig, use_container_width=True)

with col2:
    st.plotly_chart(fig_donut_volume, use_container_width=True)







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







df_stakers_distribution_volume = load_stakers_distribution_volume(start_date, end_date)
df_stakers_activity_tracker = load_stakers_activity_tracker(start_date, end_date)
