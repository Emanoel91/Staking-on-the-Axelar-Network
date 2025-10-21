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
st.title("👨‍💻Validators Analysis")

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

# --- Functions -----------------------------------------------------------------------------------------------------
# --- Row 1 ----------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_nakamoto():

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
    validators_balance_change as (
    select date_trunc(day, BLOCK_TIMESTAMP) as date, 
    VALIDATOR_ADDRESS as validator,
    sum(amount)/1e6 as balance_change
    from (
        select BLOCK_TIMESTAMP, VALIDATOR_ADDRESS, 
        case when action='undelegate' then -amount
        else amount end as amount
        from axelar.gov.fact_staking
        union all
        select BLOCK_TIMESTAMP, REDELEGATE_SOURCE_VALIDATOR_ADDRESS, 
        -amount
        from axelar.gov.fact_staking
        where action='redelegate')
    group by 1,2),
    validators_historic_holders as (
    select validator
    from validators_balance_change
    group by 1),
    user_dates as (
    select start_date, validator
    from date_start, validators_historic_holders),
    validators_balance as 
    ( select *,
    sqrt(balance) as q_power     
     from
        (select start_date as date, validator, 
        lag(balance_raw) ignore nulls over (partition by validator order by start_date) as balance_lag,
        ifnull(balance_raw, balance_lag) as balance
        from (
            select start_date, a.validator, balance_change,
            sum(balance_change) over (partition by a.validator order by start_date) as balance_raw,
            from user_dates a 
            left join validators_balance_change b 
            on date=start_date and a.validator=b.validator))
    where balance>0),
    total_staked as (
    select date, sum(BALANCE) as total_staked, sum(q_power) as total_power 
    from validators_balance
    group by 1)
    select date as "Date", min(rank) as "Nakamoto Coefficient"
    from 
    (select a.date, validator, q_power as staked, 
    sum(q_power) over(partition by a.date order by q_power desc) as cumulative_stake,
    round(100*cumulative_stake/total_power,2) as share,
    rank() over(partition by a.date order by q_power desc) as rank
    from validators_balance a 
    join total_staked b 
    on a.date=b.date
    where q_power is not null and q_power>0
    group by 1,2,3,total_power)
    where SHARE >= 33.6
    group by 1
    """
    
    df = pd.read_sql(query, conn)
    return df

# --- Load Data: Row 1 -----------------------------------------------------------------------------------------------------
df_nakamoto = load_nakamoto()
# --- Chart: Row 1 ---------------------------------------------------------------------------------------------------------
fig_b1 = go.Figure()
fig_b1.add_trace(go.Bar(x=df_nakamoto["Date"], y=df_nakamoto["Nakamoto Coefficient"], name="Nakamoto Coefficient"))
fig_b1.update_layout(barmode="stack", title="Nakamoto Coefficient (Quadratic at 33.6%) Over Time", yaxis=dict(title="Nakamoto Coefficient"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
st.plotly_chart(fig_b1, use_container_width=True)

# --- Row 2 -----------------------------------------------------------------------------------------------------------
@st.cache_data
def load_active_validators_list():

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

validators_balance_change as (
    select date_trunc(day, BLOCK_TIMESTAMP) as date, 
    VALIDATOR_ADDRESS as validator,
    sum(amount)/1e6 as balance_change
    from (
        select BLOCK_TIMESTAMP, VALIDATOR_ADDRESS, 
        case when action='undelegate' then -amount
        else amount end as amount
        from axelar.gov.fact_staking
        union all
        select BLOCK_TIMESTAMP, REDELEGATE_SOURCE_VALIDATOR_ADDRESS, 
        -amount
        from axelar.gov.fact_staking
        where action='redelegate')
    group by 1,2),

validators_stakers_change as (
    select DELEGATOR_ADDRESS as user, 
    VALIDATOR_ADDRESS as validator,
    sum(amount)/1e6 as balance
    from (
        select BLOCK_TIMESTAMP, VALIDATOR_ADDRESS, DELEGATOR_ADDRESS,
        case when action='undelegate' then -amount
        else amount end as amount
        from axelar.gov.fact_staking
        union all
        select BLOCK_TIMESTAMP, REDELEGATE_SOURCE_VALIDATOR_ADDRESS, DELEGATOR_ADDRESS,
        -amount
        from axelar.gov.fact_staking
        where action='redelegate')
    group by 1,2),

validators_historic_holders as (
    select validator
    from validators_balance_change
    group by 1),

user_dates as (
    select start_date, validator
    from date_start, validators_historic_holders),

validators_balance as 
    (select start_date as date, validator,
    lag(balance_raw) ignore nulls over (partition by validator order by start_date) as balance_lag,
    ifnull(balance_raw, balance_lag) as balance
    from (
        select start_date, a.validator, balance_change,
        sum(balance_change) over (partition by a.validator order by start_date) as balance_raw,
        from user_dates a 
        left join validators_balance_change b 
        on date=start_date and a.validator=b.validator)),
total_staked as (
select sum(BALANCE) as total_staked, sum(sqrt(BALANCE)) as total_q
from validators_balance
where date=(select max(date) from validators_balance)
and balance>0),
stakers as (
  select validator, count(distinct user) as stakers
  from validators_stakers_change
  where balance>0
  group by 1 
)
    select LABEL as "Validator",
    round(staked,2) as "Staked Amount", case
    when round(100*(staked-balance)/balance,2)<0 then '🟥 '||round(100*(staked-balance)/balance,2)||'%'
    when round(100*(staked-balance)/balance,2)>0 then '🟩 '||round(100*(staked-balance)/balance,2)||'%'
    else round(100*(staked-balance)/balance,2)||'%' end as "30D Change %",
    round(q_power,2) as "Voting Power (Quadratic)",
    stakers as "Stakers",
    share||'%' as "Cumulative Stake %",
    q_share||'%' as "Q Cumulative Stake %",
    a.validator as "Address"
    from 
    (select validator, BALANCE as staked, sqrt(staked) as q_power,
    sum(balance) over(order by balance desc) as cumulative_stake,
    sum(q_power) over(order by q_power desc) as cumulative_q_power,
    round(100*cumulative_stake/total_staked,2) as share,
    round(100*cumulative_q_power/total_q,2) as q_share
    from validators_balance,total_staked
    where date=current_date-1 and balance>0) a 
    join stakers b on a.validator=b.validator
    join validators_balance c on a.validator=c.validator and c.date=current_date-31
    left join axelar.gov.fact_validators on a.validator=ADDRESS
    order by 2 desc
    limit 75
    """

    df = pd.read_sql(query, conn)
    return df
# --- Load Data: Row 2 -----------------------------------------------------------------------------------------------------
df_active_validators_list = load_active_validators_list()
# --- Table: Row 2 ---------------------------------------------------------------------------------------------------------
st.subheader("Active Validators List")
df_display = df_active_validators_list.copy()
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)

# --- Row 3 ------------------------------------------------------------------------------------------------------------------
# فرض می‌کنیم df_active_validators_list از قبل ساخته شده است
df_chart = df_active_validators_list.copy()

# ابتدا عدد خالص درصد را استخراج کنیم (چون مقدار شامل ایموجی 🟩 و 🟥 است)
df_chart["Change_Value"] = (
    df_chart["30D Change %"]
    .str.replace("🟩", "", regex=False)
    .str.replace("🟥", "", regex=False)
    .str.replace("%", "", regex=False)
    .astype(float)
)

# رسم نمودار
fig = px.bar(
    df_chart.sort_values("Change_Value"),
    x="Change_Value",
    y="Validator",
    orientation="h",
    color=df_chart["Change_Value"].apply(lambda x: "🟩 مثبت" if x > 0 else "🟥 منفی"),
    color_discrete_map={"🟩 مثبت": "green", "🟥 منفی": "red"},
    title="تغییر ۳۰ روزه استیکینگ برای هر Validator",
)

# تنظیم ظاهر نمودار
fig.update_layout(
    xaxis_title="تغییر درصدی در ۳۰ روز گذشته (%)",
    yaxis_title="Validator",
    showlegend=False,
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
fig.update_traces(marker_line_width=0.5, marker_line_color="black")

# نمایش در Streamlit
st.subheader("📉 30D Change % per Validator")
st.plotly_chart(fig, use_container_width=True)

