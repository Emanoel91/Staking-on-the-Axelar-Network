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
st.title("🥩Staking Analysis")

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
def load_current_net_staked():

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
    axl_stakers_balance_change as (
    select * from 
        (select date_trunc(day, block_timestamp) as date, 
        user, 
        sum(amount)/1e6 as balance_change
        from 
            (
            select block_timestamp, DELEGATOR_ADDRESS as user, -1* amount as amount, TX_ID as tx_hash
            from axelar.gov.fact_staking
            where action='undelegate' and TX_SUCCEEDED=TRUE
            union all 
            select block_timestamp, DELEGATOR_ADDRESS, amount, TX_ID
            from axelar.gov.fact_staking
            where action='delegate' and TX_SUCCEEDED=TRUE)
        group by 1,2)),

    axl_stakers_historic_holders as (
    select user
    from axl_stakers_balance_change
    group by 1),

    user_dates as (
    select start_date, user
    from date_start, axl_stakers_historic_holders),

    users_balance as 
    (select start_date as "Date", user,
    lag(balance_raw) ignore nulls over (partition by user order by start_date) as balance_lag,
    ifnull(balance_raw, balance_lag) as balance
    from (
        select start_date, a.user, balance_change,
        sum(balance_change) over (partition by a.user order by start_date) as balance_raw,
        from user_dates a 
        left join axl_stakers_balance_change b 
        on date=start_date and a.user=b.user))

    select "Date", round(sum(balance)) as "Net Staked", 1215160193 as "Current Total Supply", round((100*"Net Staked"/"Current Total Supply"),2) as "Net Staked %"
    from users_balance
    where balance>=0.001 and balance is not null
    group by 1 
    order by 1 desc
    limit 1
    """

    df = pd.read_sql(query, conn)
    return df

# --- Row 2,3,4 -------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_staking_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
    select count(distinct tx_id) as "Staking Count",
    count(distinct delegator_address) as "Unique Stakers",
    round(avg(amount)/pow(10,6)) as "Average", 
    round(median(amount)/pow(10,6)) as "Median", 
    round(max(amount)/pow(10,6)) as "Maximum",
    round((sum(amount)/pow(10,6))/count(distinct delegator_address)) as "Avg Staking Volume per User",
    round(count(distinct tx_id)/count(distinct delegator_address)) as "Avg Staking Count per User"
   from axelar.gov.fact_staking
   where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
   block_timestamp::date<='{end_str}' and action='delegate'),
   table2 as (with tab1 as (select delegator_address, round(sum(amount)/pow(10,6)) as tot_staking_vol
   from axelar.gov.fact_staking
   where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
   block_timestamp::date<='{end_str}' and action='delegate'
   group by 1)
   select round(median(tot_staking_vol)) as "Median Volume of Tokens Staked by Users",
   round(max(tot_staking_vol)) as "Max Volume of Tokens Staked by User"
   from tab1)
   select * from table1 , table2
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row --------------------------------------------------------------------------------------------------------
df_current_net_staked = load_current_net_staked()
df_staking_stats = load_staking_stats(start_date, end_date)
# --- KPIs: Row 1,2,3,4 ---------------------------------------------------------------------------------------------------
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
    st.markdown(card_style.format(label="Current Net Staked", value=f"{df_current_net_staked["Net Staked"][0]:,} $AXL"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="%Staked-to-Total Supply", value=f"{df_current_net_staked["Net Staked %"][0]:,}%"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Current Total Supply", value=f"{df_current_net_staked["Current Total Supply"][0]:,} $AXL"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col4, col5, col6 = st.columns(3)
with col4:
    st.markdown(card_style.format(label="Staking Transactions", value=f"{df_staking_stats["Staking Count"][0]:,} Txns"), unsafe_allow_html=True)
with col5:
    st.markdown(card_style.format(label="Unique Stakers", value=f"{df_staking_stats["Unique Stakers"][0]:,} Wallets"), unsafe_allow_html=True)
with col6:
    st.markdown(card_style.format(label="Avg Staking Count per Wallet", value=f"{df_staking_stats["Avg Staking Count per User"][0]:,} Txns"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col7, col8, col9 = st.columns(3)
with col7:
    st.markdown(card_style.format(label="Avg Staking Amount per Txn", value=f"{df_staking_stats["Average"][0]:,} $AXL"), unsafe_allow_html=True)
with col8:
    st.markdown(card_style.format(label="Median Staking Amount per Txn", value=f"{df_staking_stats["Median"][0]:,} $AXL"), unsafe_allow_html=True)
with col9:
    st.markdown(card_style.format(label="Max Staking Amount per Txn", value=f"{df_staking_stats["Maximum"][0]:,} $AXL"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col10, col11, col12 = st.columns(3)
with col10:
    st.markdown(card_style.format(label="Avg Staking Amount per Wallet", value=f"{df_staking_stats["Avg Staking Volume per User"][0]:,} $AXL"), unsafe_allow_html=True)
with col11:
    st.markdown(card_style.format(label="Median Staking Amount per Wallet", value=f"{df_staking_stats["Median Volume of Tokens Staked by Users"][0]:,} $AXL"), unsafe_allow_html=True)
with col12:
    st.markdown(card_style.format(label="Max Staking Amount per Wallet", value=f"{df_staking_stats["Max Volume of Tokens Staked by User"][0]:,} $AXL"), unsafe_allow_html=True)

# --- Row 5 ----------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_net_staked_overtime(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    query = f"""
    with overview as (
    with date_start as (
    with dates AS (
    SELECT CAST('2022-02-10' AS DATE) AS start_date 
    UNION ALL
    SELECT DATEADD(day, 1, start_date)
    FROM dates
    WHERE start_date < CURRENT_DATE())
    SELECT date_trunc(day, start_date) AS start_date
    FROM dates),
    axl_stakers_balance_change as (
    select * from 
        (select date_trunc(day, block_timestamp) as date, 
        user, 
        sum(amount)/1e6 as balance_change
        from 
            (
            select block_timestamp, DELEGATOR_ADDRESS as user, -1* amount as amount, TX_ID as tx_hash
            from axelar.gov.fact_staking
            where action='undelegate' and TX_SUCCEEDED=TRUE
            union all 
            select block_timestamp, DELEGATOR_ADDRESS, amount, TX_ID
            from axelar.gov.fact_staking
            where action='delegate' and TX_SUCCEEDED=TRUE)
        group by 1,2)),

    axl_stakers_historic_holders as (
    select user
    from axl_stakers_balance_change
    group by 1),

    user_dates as (
    select start_date, user
    from date_start, axl_stakers_historic_holders),

    users_balance as 
    (select start_date as "Date", user,
    lag(balance_raw) ignore nulls over (partition by user order by start_date) as balance_lag,
    ifnull(balance_raw, balance_lag) as balance
    from (
        select start_date, a.user, balance_change,
        sum(balance_change) over (partition by a.user order by start_date) as balance_raw,
        from user_dates a 
        left join axl_stakers_balance_change b 
        on date=start_date and a.user=b.user))

    select "Date", round(sum(balance)) as "Net Staked", 1215160193 as "Current Total Supply", round((100*"Net Staked"/"Current Total Supply"),2) as "Net Staked %"
    from users_balance
    where balance>=0.001 and balance is not null
    group by 1 
    order by 1 desc)
    select "Date", "Net Staked"
    from overview
    where "Date">='{start_str}' and "Date"<='{end_str}'
    order by 1
    """
    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 5 ----------------------------------------------------------------------------------------
df_net_staked_overtime = load_net_staked_overtime(start_date, end_date)
# --- Charts 5 ------------------------------------------------------------------------------------------------

fig = px.area(df_net_staked_overtime, x="Date", y="Net Staked", title="AXL Net Staked Amount Over Time")
fig.update_layout(xaxis_title="", yaxis_title="$AXL", template="plotly_white")
st.plotly_chart(fig, use_container_width=True)

# --- Row 6 -------------------------------------------------------------------------------------------------------------

@st.cache_data
def load_staking_overtime(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    select date_trunc('{timeframe}',block_timestamp) as "Date", 
    round(sum(amount)/pow(10,6)) as "Staking Volume", 
    count(distinct tx_id) as "Staking Count",
    sum("Staking Volume") over (order by "Date" asc) as "Total Staking Volume", 
    sum("Staking Count") over (order by "Date" asc) as "Total Staking Count",
    round(avg(amount)/pow(10,6)) as "Avg Volume per Txn", 
    round((sum(amount)/pow(10,6))/count(distinct delegator_address)) as "Avg Volume per User"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
    block_timestamp::date<='{end_str}' and action='delegate'
    group by 1
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 6 ---------------------------------------------------------------------------------------------------
df_staking_overtime = load_staking_overtime(timeframe, start_date, end_date)
# --- Charts: Row 6 ------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_bar(x=df_staking_overtime["Date"], y=df_staking_overtime["Staking Count"], name="Staking Count", yaxis="y1", marker_color="blue")
    fig1.add_trace(go.Scatter(x=df_staking_overtime["Date"], y=df_staking_overtime["Total Staking Count"], name="Total Staking Count", mode="lines", 
                              yaxis="y2", line=dict(color="black")))
    fig1.update_layout(title="AXL Staking Count Over Time", yaxis=dict(title="Txns count"), yaxis2=dict(title="Txns count", overlaying="y", side="right"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df_staking_overtime["Date"], y=df_staking_overtime["Avg Volume per Txn"], name="Average Volume per Txn", mode="lines", 
                              yaxis="y1", line=dict(color="blue")))
    fig2.add_trace(go.Scatter(x=df_staking_overtime["Date"], y=df_staking_overtime["Avg Volume per User"], name="Average Volume per User", mode="lines", 
                              yaxis="y2", line=dict(color="green")))
    fig2.update_layout(title="Average Staking Volume Over Time", yaxis=dict(title="$AXL"), yaxis2=dict(title="$AXL", overlaying="y", side="right"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig2, use_container_width=True)

# --- Row 7 ---------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_staking_stats_different_time_frame():

    query = f"""
    with tab1 as (select count(distinct tx_id) as "Stake Count", count(distinct delegator_address) as "Staker Count",
    round(sum(amount)/pow(10,6)) as "Staking Volume", '24h' as "Time Frame"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date=current_date-1 and action='delegate'),

    tab2 as (select count(distinct tx_id) as "Stake Count", count(distinct delegator_address) as "Staker Count",
    round(sum(amount)/pow(10,6)) as "Staking Volume", '7d' as "Time Frame"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>=current_date-6 and action='delegate'),

    tab3 as (select count(distinct tx_id) as "Stake Count", count(distinct delegator_address) as "Staker Count",
    round(sum(amount)/pow(10,6)) as "Staking Volume", '30d' as "Time Frame"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>=current_date-29 and action='delegate'),

    tab4 as (select count(distinct tx_id) as "Stake Count", count(distinct delegator_address) as "Staker Count", 
    round(sum(amount)/pow(10,6)) as "Staking Volume", '1y' as "Time Frame"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>=current_date-364 and action='delegate')

    select * from tab1 union all
    select * from tab2 union all
    select * from tab3 union all 
    select * from tab4
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 7 --------------------------------------------------------------------------------------------------
df_staking_stats_different_time_frame = load_staking_stats_different_time_frame()
# --- Charts: Row 7 -----------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_bar(x=df_staking_stats_different_time_frame["Time Frame"], y=df_staking_stats_different_time_frame["Stake Count"], name="Stake Count", yaxis="y1", marker_color="blue")
    fig1.add_trace(go.Scatter(x=df_staking_stats_different_time_frame["Time Frame"], y=df_staking_stats_different_time_frame["Staker Count"], name="Staker Count", mode="lines", 
                              yaxis="y2", line=dict(color="black")))
    fig1.update_layout(title="Staking Transaction & Staker Count by Timeframe", yaxis=dict(title="Txn count"), yaxis2=dict(title="Wallet count", overlaying="y", side="right"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_bar(x=df_staking_stats_different_time_frame["Time Frame"], y=df_staking_stats_different_time_frame["Staking Volume"], name="Staking Volume", yaxis="y1", marker_color="blue")
    fig2.update_layout(title="Staking Volume by Timeframe", yaxis=dict(title="$AXL"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig2, use_container_width=True)
    




@st.cache_data
def load_txn_distribution_volume(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
     with tab1 as (select tx_id, case 
     when (amount/pow(10,6))<=1 then 'V<=1 AXL'
     when (amount/pow(10,6))>1 and (amount/pow(10,6))<=10 then '1<V<=10 AXL' 
     when (amount/pow(10,6))>10 and (amount/pow(10,6))<=100 then '10<V<=100 AXL'
     when (amount/pow(10,6))>100 and (amount/pow(10,6))<=1000 then '100<V<=1k AXL'
     when (amount/pow(10,6))>1000 and (amount/pow(10,6))<=10000 then '1k<V<=10k AXL'
     when (amount/pow(10,6))>10000 and (amount/pow(10,6))<=100000 then '10k<V<=100k AXL'
     when (amount/pow(10,6))>100000 and (amount/pow(10,6))<=1000000 then '100k<V<=1M AXL'
     else 'V>1M AXL' end as "Staking Amount"
     from axelar.gov.fact_staking
     where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='2025-01-01' AND
     block_timestamp::date<='2025-09-30' and action='delegate')
     select "Staking Amount", count(distinct tx_id) as "Txns Count"
     from tab1
     group by 1
    """

    df = pd.read_sql(query, conn)
    return df

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



df_txn_distribution_volume = load_txn_distribution_volume(start_date, end_date)
df_stakers_overtime = load_stakers_overtime(timeframe, start_date, end_date)
df_stakers_distribution_count = load_stakers_distribution_count(start_date, end_date)
df_stakers_distribution_class = load_stakers_distribution_class(start_date, end_date)
df_stakers_distribution_volume = load_stakers_distribution_volume(start_date, end_date)
df_stakers_activity_tracker = load_stakers_activity_tracker(start_date, end_date)
