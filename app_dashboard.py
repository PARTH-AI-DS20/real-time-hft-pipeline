import streamlit as st
import duckdb
import pandas as pd
import time
import plotly.graph_objects as go

# --- CONFIGURATION ---
DB_FILE = "stock_market.duckdb"

st.set_page_config(page_title="HFT Analytics Dashboard", layout="wide")

def load_live_data():
    """Reads from DuckDB with retry logic for Windows file locks [3]"""
    for attempt in range(5):
        try:
            with duckdb.connect(database=DB_FILE, read_only=True) as conn:
                query = "SELECT * FROM stock_analytics ORDER BY timestamp DESC LIMIT 1500"
                return conn.execute(query).df()
        except Exception:
            time.sleep(0.2)
    return pd.DataFrame()

# --- UI LAYOUT ---
st.title("📈 Real-Time Stock Market Intelligence")

# Fixed asset list selection
symbols = list(("NVDA", "AAPL", "TSLA", "BTC"))
selected_asset = st.sidebar.selectbox("Select Asset to Monitor", symbols)

placeholder = st.empty()

# --- MAIN REAL-TIME LOOP ---
while True:
    df_raw = load_live_data()
    
    if not df_raw.empty:
        df = df_raw[df_raw['symbol'] == selected_asset].copy()
        df = df.sort_values('timestamp')

        if not df.empty:
            with placeholder.container():
                # 1. Financial KPIs
                latest = df.iloc[-1]
                prev = df.iloc[-2] if len(df) > 1 else latest
                c1, c2, c3, c4 = st.columns(4)
                change = latest['price'] - prev['price']
                
                c1.metric(f"Live {selected_asset}", f"${latest['price']:.2f}", f"{change:.2f}")
                c2.metric("20-Tick SMA", f"${latest['sma_20']:.2f}")
                c3.metric("Volume", f"{int(latest['volume'])}")
                c4.metric("Volatility", f"{df['price'].std():.4f}")

                # 2. Price vs SMA Chart
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df['timestamp'], y=df['price'], name='Price', line=dict(color='#1f77b4')))
                fig.add_trace(go.Scatter(x=df['timestamp'], y=df['sma_20'], name='SMA-20', line=dict(dash='dot', color='#ff7f0e')))
                fig.update_layout(height=450, margin=dict(l=0, r=0, t=0, b=0))
                
                # FIX: Added unique key using timestamp to prevent StreamlitDuplicateElementId 
                chart_key = f"hft_plotly_{selected_asset}_{time.time()}"
                st.plotly_chart(fig, use_container_width=True, key=chart_key)

                # 3. Raw Data Preview
                with st.expander("View Recent Ticks"):
                    st.dataframe(df.tail(10), use_container_width=True)
        else:
            st.info(f"Awaiting stream for {selected_asset}...")
    else:
        st.warning("Connecting to stream database...")

    time.sleep(1) # Frequency of dashboard refresh