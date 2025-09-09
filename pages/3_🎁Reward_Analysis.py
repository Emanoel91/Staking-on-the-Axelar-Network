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
st.title("🎁Reward Analysis")

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
# --- Row 1,2,3 -----------------------------------------------------------------------------------------------------
@st.cache_data
def load_claim_reward_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    select 
    count(distinct delegator_address) as "Reward Claimers", 
    round(sum(amount)/pow(10,6)) as "Reward Claimed", 
    round((avg(amount)/pow(10,6)),2) as "Average", 
    round((median(amount)/pow(10,6)),2) as "Median", 
    ROUND(((sum(amount)/pow(10,6))/count(distinct delegator_address)),2) AS "Avg Reward Claimed per User",
    round(max(amount)/pow(10,6)) as "Maximum",
    count(distinct tx_id) as "Claim Txns Count"
    from axelar.gov.fact_staking_rewards
    where block_timestamp::date>='{start_str}' and block_timestamp::date<='{end_str}' and tx_succeeded='true'
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_claim_reward_stats_user(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with tab1 as (select 
    delegator_address, sum(amount)/pow(10,6) as reward_claimed
    from axelar.gov.fact_staking_rewards
    where block_timestamp::date>='{start_str}' and block_timestamp::date<='{end_str}' and tx_succeeded='true'
    group by 1)
    select round(median(reward_claimed),2) as "Median Reward Claimed by Users", round(max(reward_claimed)) as "Max Reward"
    from tab1
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 1,2,3 ---------------------------------------------
df_claim_reward_stats = load_claim_reward_stats(start_date, end_date)
df_claim_reward_stats_user = load_claim_reward_stats_user(start_date, end_date)
# --- kpis: Row 1,2,3 --------------------------------------------------
card_style = """
    <div style="
        background-color: #f9f9f9;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.05);
        ">
        <h4 style="margin: 0; font-size: 20px; color: #555;">{label}</h4>
        <p style="margin: 5px 0 0; font-size: 20px; font-weight: bold; color: #000;">{value}</p>
    </div>
"""

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(card_style.format(label="Unique Claimers", value=f"{df_claim_reward_stats["Reward Claimers"][0]:,} Wallets"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="Claim Transactions", value=f"{df_claim_reward_stats["Claim Txns Count"][0]:,} Txns"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Total Reward Claimed", value=f"{df_claim_reward_stats["Reward Claimed"][0]:,} $AXL"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col4, col5, col6 = st.columns(3)
with col4:
    st.markdown(card_style.format(label="Avg Reward per Txn", value=f"{df_claim_reward_stats["Average"][0]:,} $AXL"), unsafe_allow_html=True)
with col5:
    st.markdown(card_style.format(label="Median Reward per Txn", value=f"{df_claim_reward_stats["Median"][0]:,} $AXL"), unsafe_allow_html=True)
with col6:
    st.markdown(card_style.format(label="Max Reward per Txn", value=f"{df_claim_reward_stats["Maximum"][0]:,} $AXL"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col7, col8, col9 = st.columns(3)
with col7:
    st.markdown(card_style.format(label="Avg Reward per Wallet", value=f"{df_claim_reward_stats["Avg Reward Claimed per User"][0]:,} $AXL"), unsafe_allow_html=True)
with col8:
    st.markdown(card_style.format(label="Median Reward per Wallet", value=f"{df_claim_reward_stats_user["Median Reward Claimed by Users"][0]:,} $AXL"), unsafe_allow_html=True)
with col9:
    st.markdown(card_style.format(label="Max Reward per Wallet", value=f"{df_claim_reward_stats_user["Max Reward"][0]:,} $AXL"), unsafe_allow_html=True)

# --- Row 4,5 -----------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_reward_stats_overtime(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    select date_trunc('{timeframe}',block_timestamp) as "Date", 
    count(distinct delegator_address) as "Reward Claimers", 
    round(sum(amount)/pow(10,6)) as "Reward Claimed",
    sum("Reward Claimed") over (order by "Date" asc) as "Total Reward Claimed", 
    round((avg(amount)/pow(10,6)),1) as "Average", 
    round((median(amount)/pow(10,6)),1) as "Median", 
    round(max(amount)/pow(10,6)) as "Maximum",
    count(distinct tx_id) as "Claim Txns Count", 
    sum("Claim Txns Count") over (order by "Date" asc) as "Total TXs Count"
    from axelar.gov.fact_staking_rewards
    where block_timestamp::date>='{start_str}' and block_timestamp::date<='{end_str}' and tx_succeeded='true'
    group by 1
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------------
df_reward_stats_overtime = load_reward_stats_overtime(timeframe, start_date, end_date)
# --- Charts: Row 4,5 ----------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_bar(x=df_reward_stats_overtime["Date"], y=df_reward_stats_overtime["Reward Claimed"], name="Reward Amount", yaxis="y1", marker_color="orange")
    fig1.add_trace(go.Scatter(x=df_reward_stats_overtime["Date"], y=df_reward_stats_overtime["Total Reward Claimed"], name="Total Reward Amount", mode="lines", 
                              yaxis="y2", line=dict(color="black")))
    fig1.update_layout(title="Amount of Rewards Claimed Over Time", yaxis=dict(title="$AXL"), yaxis2=dict(title="$AXL", overlaying="y", side="right"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_bar(x=df_reward_stats_overtime["Date"], y=df_reward_stats_overtime["Claim Txns Count"], name="Claim Txns", yaxis="y1", marker_color="orange")
    fig2.add_trace(go.Scatter(x=df_reward_stats_overtime["Date"], y=df_reward_stats_overtime["Reward Claimers"], name="Reward Claimers", mode="lines", yaxis="y2", 
                              line=dict(color="black")))
    fig2.update_layout(title="Claim Txns & Reward Claimers Over Time", yaxis=dict(title="Txns count"), yaxis2=dict(title="Wallet count", overlaying="y", side="right"), 
                       xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=df_reward_stats_overtime["Date"], y=df_reward_stats_overtime["Average"], name="Average Reward per Txn", mode="lines", 
                              yaxis="y1", line=dict(color="blue")))
    fig3.add_trace(go.Scatter(x=df_reward_stats_overtime["Date"], y=df_reward_stats_overtime["Median"], name="Median Reward per Txn", mode="lines", 
                              yaxis="y2", line=dict(color="green")))
    fig3.update_layout(title="Average & Median Reward Claimed per Txn Over Time", yaxis=dict(title="$AXL"), yaxis2=dict(title="$AXL", overlaying="y", side="right"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    fig4 = go.Figure()
    fig4.add_bar(x=df_reward_stats_overtime["Date"], y=df_reward_stats_overtime["Maximum"], name="Maximum Reward per Txn", yaxis="y1", marker_color="red")
    fig4.update_layout(title="Maximum Reward Claimed per Txn Over Time", yaxis=dict(title="$AXL"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig4, use_container_width=True)

# --- Row 6 -----------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_distribution_claimer_volume(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with tab1 as (select delegator_address, round(sum(amount)/pow(10,6)) as "Reward Volume", case 
    when (sum(amount)/pow(10,6))<=10 then 'V<=10 AXL'
    when (sum(amount)/pow(10,6))>10 and (sum(amount)/pow(10,6))<=100 then '10<V<=100 AXL'
    when (sum(amount)/pow(10,6))>100 and (sum(amount)/pow(10,6))<=1000 then '100<V<=1k AXL'
    when (sum(amount)/pow(10,6))>1000 and (sum(amount)/pow(10,6))<=10000 then '1k<V<=10k AXL'
    when (sum(amount)/pow(10,6))>10000 and (sum(amount)/pow(10,6))<=100000 then '10k<V<=100k AXL'
    when (sum(amount)/pow(10,6))>100000 and (sum(amount)/pow(10,6))<=1000000 then '100k<V<=1M AXL'
    else 'V>1M AXL' end as "Class"
    from axelar.gov.fact_staking_rewards
    where block_timestamp::date>='{start_str}' and block_timestamp::date<='{end_str}' and tx_succeeded='true'
    group by 1)
    select "Class", count(distinct delegator_address) as "Staker Count"
    from tab1
    group by 1
    order by 2 desc 
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_distribution_txn_volume(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with tab1 as (select tx_id, round(sum(amount)/pow(10,6)) as "Reward Volume", case 
    when (sum(amount)/pow(10,6))<=1 then 'V<=1 AXL'
    when (sum(amount)/pow(10,6))>1 and (sum(amount)/pow(10,6))<=5 then '1<V<=5 AXL'
    when (sum(amount)/pow(10,6))>5 and (sum(amount)/pow(10,6))<=10 then '5<V<=10 AXL'
    when (sum(amount)/pow(10,6))>10 and (sum(amount)/pow(10,6))<=100 then '10<V<=100 AXL'
    when (sum(amount)/pow(10,6))>100 and (sum(amount)/pow(10,6))<=1000 then '100<V<=1k AXL'
    when (sum(amount)/pow(10,6))>1000 and (sum(amount)/pow(10,6))<=10000 then '1k<V<=10k AXL'
    when (sum(amount)/pow(10,6))>10000 and (sum(amount)/pow(10,6))<=100000 then '10k<V<=100k AXL'
    when (sum(amount)/pow(10,6))>100000 and (sum(amount)/pow(10,6))<=1000000 then '100k<V<=1M AXL'
    else 'V>1M AXL' end as "Class"
    from axelar.gov.fact_staking_rewards
    where block_timestamp::date>='{start_str}' and block_timestamp::date<='{end_str}' and tx_succeeded='true'
    group by 1)
    select "Class", count(distinct tx_id) as "Stake Count"
    from tab1
    group by 1
    order by 2 desc 
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data Row 6 -------------------------------------------------------------------------------------------------
df_distribution_claimer_volume = load_distribution_claimer_volume(start_date, end_date)
df_distribution_txn_volume = load_distribution_txn_volume(start_date, end_date)

# ---Charts: Row 6 ---------------------------------------------------------------------------------------------------------------

fig_donut_claimer_volume = px.pie(df_distribution_claimer_volume, names="Class", values="Staker Count", title="Distribution of Stakers by Reward Amount", hole=0.5, color="Staker Count")
fig_donut_claimer_volume.update_traces(textposition='inside', textinfo='percent', pull=[0.05]*len(df_distribution_claimer_volume))
fig_donut_claimer_volume.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

# -----------------------

fig_donut_txn_volume = px.pie(df_distribution_txn_volume, names="Class", values="Stake Count", title="Distribution of Staking Transactions by Reward Amount", hole=0.5, 
                              color="Stake Count")
fig_donut_txn_volume.update_traces(textposition='inside', textinfo='percent', pull=[0.05]*len(df_distribution_txn_volume))
fig_donut_txn_volume.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(fig_donut_claimer_volume, use_container_width=True)

with col2:
    st.plotly_chart(fig_donut_txn_volume, use_container_width=True)

# --- Row 7 ---------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_top_reward_claimers(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    select delegator_address as "Delegator", 
    round(sum(amount)/pow(10,6)) as "Reward Volume", 
    count(distinct tx_id) as "Reward Claimed Txns",
    min(block_timestamp::date) as "First Reward Claim Date",
    round(avg(amount)/pow(10,6)) as "Avg Reward Claimed"
    from axelar.gov.fact_staking_rewards
    where block_timestamp::date>='{start_str}' and block_timestamp::date<='{end_str}' and tx_succeeded='true'
    group by 1
    order by 2 desc 
    limit 1000
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 7 -------------------------------------------------------------------------------------------------
df_top_reward_claimers = load_top_reward_claimers(start_date, end_date)

# --- Table: Row 7 ----------------------------------------------------------------------------------------------------
st.subheader("🏆 Top Reward Claimers")
df_display = df_top_reward_claimers.copy()
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)

# --- Row 8 -------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_recent_claim_stats():

    query = f"""
    select block_timestamp::date as "📅Date", delegator_address as "👨‍💼Delegator", 
    (amount)/pow(10,6) as "💰Reward Volume"
    from axelar.gov.fact_staking_rewards
    where tx_succeeded='true' and block_timestamp::date = current_date - 1
    order by 1 desc 
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 8 -------------------------------------------------------------------------------------------------
df_recent_claim_stats = load_recent_claim_stats()

# --- Table: Row 8 ----------------------------------------------------------------------------------------------------
st.subheader("📋 Recent Reward Claims")
df_display = df_recent_claim_stats.copy()
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)
