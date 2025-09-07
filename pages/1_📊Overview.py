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
st.title("📊Overview")

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

# --- Row 1,2,3 ---------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_staking_over_time(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    select date_trunc('{timeframe}',block_timestamp) as "Date", action as "Action", 
    round(sum(amount)/pow(10,6)) as "Txn Volume", count(distinct tx_id) as "Txn Count",
    count(distinct delegator_address) as "User Count",
    round(avg(amount)/pow(10,6)) as "Average", round(median(amount)/pow(10,6)) as "Median", 
    round(max(amount)/pow(10,6)) as "Maximum"
    from axelar.gov.fact_staking
    where tx_succeeded='true' and currency='uaxl' and block_timestamp::date>='{start_str}' AND
    block_timestamp::date<='{end_str}'
    group by 1,2
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df
# --- Load Data: Row 1,2,3 ---------------------------------------------------
df_staking_over_time = load_staking_over_time(timeframe, start_date, end_date)
# --- Charts: Row 1 ----------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig_stacked_volume = px.bar(
        df_staking_over_time,
        x="Date",
        y="Txn Volume",
        color="Action",
        title="Transactions Volume Over Time By Action"
    )
    fig_stacked_volume.update_layout(barmode="stack", yaxis_title="$USD", xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5, title=""))
    st.plotly_chart(fig_stacked_volume, use_container_width=True)

with col2:
    fig_stacked_txn = px.bar(
        df_staking_over_time,
        x="Date",
        y="Txn Count",
        color="Action",
        title="Transactions Count Over Time By Action"
    )
    fig_stacked_txn.update_layout(barmode="stack", yaxis_title="Txns count", xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig_stacked_txn, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    fig_line_user = px.line(
        df_staking_over_time,
        x="Date",
        y="User Count",
        color="Action",
        title="User Count over Time By Action"
    )
    fig_line_user.update_layout(yaxis_title="Wallet count", xaxis_title="")
    st.plotly_chart(fig_line_user, use_container_width=True)

with col4:
    fig_line_median = px.line(
        df_staking_over_time,
        x="Date",
        y="Median",
        color="Action",
        title="Median Transactions Volume Over Time By Action"
    )
    fig_line_median.update_layout(yaxis_title="$USD", xaxis_title="")
    st.plotly_chart(fig_line_median, use_container_width=True)

col5, col6 = st.columns(2)

with col5:
    fig_norm_stacked_volume = px.bar(
       df_staking_over_time,
       x="Date",
       y="Txn Volume",
       color="Action",
       title="Transactions Volume Over Time By Action (%Normalized)",
       text="Txn Volume",
    )

    fig_norm_stacked_volume.update_layout(barmode='stack', uniformtext_minsize=8, uniformtext_mode='hide')
    fig_norm_stacked_volume.update_traces(textposition='inside')

    fig_norm_stacked_volume.update_layout(yaxis=dict(tickformat='%'))
    fig_norm_stacked_volume.update_traces(hovertemplate='%{y} Transfers<br>%{x}<br>%{color}')

    df_norm = df_staking_over_time.copy()
    df_norm['total_per_date'] = df_norm.groupby('Date')['Txn Volume'].transform('sum')
    df_norm['normalized'] = df_norm['Txn Volume'] / df_norm['total_per_date']

    fig_norm_stacked_volume = px.bar(
       df_norm,
       x='Date',
       y='normalized',
       color='Action',
       title="Transactions Volume Over Time By Action (%Normalized)",
       text=df_norm['Txn Volume'].astype(str),
    )

    fig_norm_stacked_volume.update_layout(barmode='stack')
    fig_norm_stacked_volume.update_traces(textposition='inside')
    fig_norm_stacked_volume.update_yaxes(tickformat='%')
    st.plotly_chart(fig_norm_stacked_volume, use_container_width=True)

with col6:
    fig_norm_stacked_txn = px.bar(
       df_staking_over_time,
       x="Date",
       y="Txn Count",
       color="Action",
       title="Transactions Count Over Time By Action (%Normalized)",
       text="Txn Count",
    )

    fig_norm_stacked_txn.update_layout(barmode='stack', uniformtext_minsize=8, uniformtext_mode='hide')
    fig_norm_stacked_txn.update_traces(textposition='inside')

    fig_norm_stacked_txn.update_layout(yaxis=dict(tickformat='%'))
    fig_norm_stacked_txn.update_traces(hovertemplate='%{y} Transfers<br>%{x}<br>%{color}')

    df_norm = df_staking_over_time.copy()
    df_norm['total_per_date'] = df_norm.groupby('Date')['Txn Count'].transform('sum')
    df_norm['normalized'] = df_norm['Txn Count'] / df_norm['total_per_date']

    fig_norm_stacked_txn = px.bar(
       df_norm,
       x='Date',
       y='normalized',
       color='Action',
       title="Transactions Count Over Time By Action (%Normalized)",
       text=df_norm['Txn Count'].astype(str),
    )

    fig_norm_stacked_txn.update_layout(barmode='stack')
    fig_norm_stacked_txn.update_traces(textposition='inside')
    fig_norm_stacked_txn.update_yaxes(tickformat='%')
    st.plotly_chart(fig_norm_stacked_txn, use_container_width=True)

