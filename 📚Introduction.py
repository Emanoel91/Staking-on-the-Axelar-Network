import streamlit as st

# --- Page Config: Tab Title & Icon ---
st.set_page_config(
    page_title="Staking on the Axelar Network",
    page_icon="https://pbs.twimg.com/profile_images/1877235283755778048/4nlylmxm_400x400.jpg",
    layout="wide"
)

# --- Title with Logo ---
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://pbs.twimg.com/profile_images/1877235283755778048/4nlylmxm_400x400.jpg" alt="axelar" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Staking on the Axelar Network</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Builder Info ------------------------------------------------------------------------

st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz" style="width:25px; height:25px; border-radius: 50%;">
            <span>Built by: <a href="https://x.com/0xeman_raz" target="_blank">Eman Raz</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Info Box ---
st.markdown(
    """
    <div style="background-color: #fec7a2; padding: 15px; border-radius: 10px; border: 1px solid #fec7a2;">
        Axelar's staking mechanism is an important component of its blockchain network. The staking process involves AXL token holders participating in the 
        network's Proof-of-Stake (PoS) consensus protocol. Validators stake their AXL tokens to verify cross-chain transactions and maintain the security and 
        integrity of the network. To stake AXL tokens, users need to ensure they have native AXL tokens held in a Cosmos-compatible wallet that supports staking, 
        such as Ledger, Keplr, or Cosmostation. If the AXL tokens are held in an EVM wallet like MetaMask, they need to be converted to native AXL.
        Once users have native AXL tokens, they can choose a validator to stake their tokens with on the Axelar network. This can be done through a compatible 
        wallet's staking dashboard. Users select the Axelar network, choose a validator, enter the amount of AXL tokens they wish to stake, and approve the transaction. By staking AXL tokens, users contribute to the security and operation of the Axelar network while earning rewards for their participation.
</div>
    """,
    unsafe_allow_html=True
)

# --- Reference and Rebuild Info ---
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1856738793325268992/OouKI10c_400x400.jpg" alt="Flipside" style="width:25px; height:25px; border-radius: 50%;">
            <span>Data Powered by: <a href="https://flipsidecrypto.xyz/home/" target="_blank">Flipside</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Links with Logos ---
st.markdown(
    """
    <div style="font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://axelarscan.io/logos/logo.png" alt="Axelar" style="width:20px; height:20px;">
            <a href="https://www.axelar.network/" target="_blank">Axelar Website</a>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://axelarscan.io/logos/logo.png" alt="X" style="width:20px; height:20px;">
            <a href="https://x.com/axelar" target="_blank">Axelar X Account</a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

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
