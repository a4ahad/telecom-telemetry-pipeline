import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Page Configuration & Theme Vibe
st.set_page_config(
    page_title="NOC Core - Telemetry Lakehouse", 
    page_icon="⚡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a dark, sleek enterprise styling
st.markdown("""
    <style>
    .reportview-container { background: #0E1117; }
    div[data-testid="stMetricValue"] { font-size: 2rem; font-weight: 700; color: #00FFCC; }
    div[data-testid="stMetricLabel"] { font-size: 0.9rem; color: #9CA3AF; }
    .stAlert { border-radius: 8px; }
    </style>
""", unsafe_allow_html=True)

# 2. Optimized Data Ingestion Layer (Auto-resolves Spark Partitions)
@st.cache_data(ttl=60)  # Cache for 1 minute to stay close to live data pipelines
def load_lakehouse_data():
    detailed_dir = "data/processed/detailed_telemetry"
    profile_dir = "data/processed/device_profiles"
    
    # Direct directory reading parses partition folders into dataframe columns automatically
    if os.path.exists(detailed_dir):
        df_detailed = pd.read_parquet(detailed_dir)
    else:
        df_detailed = pd.DataFrame()
        
    if os.path.exists(profile_dir):
        df_profile = pd.read_parquet(profile_dir)
    else:
        df_profile = pd.DataFrame()
        
    return df_detailed, df_profile

with st.spinner("🔄 Streaming latest records from Gold Optimization Layer..."):
    df_detailed, df_profile = load_lakehouse_data()

# --- SIDEBAR INTERACTIVE FILTERS ---
st.sidebar.header("📡 NOC Global Filters")
st.sidebar.markdown("Tailor your infrastructure viewport down to specific assets.")

if not df_detailed.empty:
    # 1. Multi-select for Partitioned Status Codes
    unique_statuses = df_detailed['status_code'].unique().tolist()
    selected_statuses = st.sidebar.multiselect(
        "Filter by Operational Status:",
        options=unique_statuses,
        default=unique_statuses,
        help="Queries target directory paths instantly via partition pruning."
    )
    
    # 2. Search box for specific hardware items
    search_device = st.sidebar.text_input("🔍 Quick Asset Lookup (e.g., DEV-1078):").strip()
    
    # 3. Dynamic Slider for Latency Thresholds
    max_latency_bound = float(df_profile['avg_latency_ms'].max()) if not df_profile.empty else 100.0
    latency_threshold = st.sidebar.slider(
        "Minimum Latency Floor (ms):",
        min_value=0.0,
        max_value=max_latency_bound,
        value=0.0
    )

    # Apply global interactive filtering logic
    df_filtered_detailed = df_detailed[df_detailed['status_code'].isin(selected_statuses)]
    df_filtered_profile = df_profile[df_profile['avg_latency_ms'] >= latency_threshold]
    
    if search_device:
        df_filtered_detailed = df_filtered_detailed[df_filtered_detailed['device_id'].str.contains(search_device, case=False)]
        df_filtered_profile = df_filtered_profile[df_filtered_profile['device_id'].str.contains(search_device, case=False)]
else:
    df_filtered_detailed = pd.DataFrame()
    df_filtered_profile = pd.DataFrame()


# --- HEADER SECTION ---
st.title("⚡ NOC Fleet Telemetry Operations Center")
st.markdown("Automated edge telemetry analytics optimized via PySpark Medallion Engine.")
st.markdown("---")


# --- KPI METRICS BANNER ---
if not df_filtered_profile.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    total_assets = df_filtered_profile['device_id'].nunique()
    active_alerts = df_filtered_profile['hardware_alert_flag'].sum()
    avg_latency = df_filtered_profile['avg_latency_ms'].mean()
    avg_dbm_signal = df_filtered_profile['avg_dbm'].mean()
    
    col1.metric("Assets in Viewport", f"{total_assets:,}")
    col2.metric("Hardware Alert Flags ⚠️", f"{active_alerts}", delta=f"{active_alerts} Urgent Dispatches")
    col3.metric("Avg Latency Core", f"{avg_latency:.2f} ms")
    col4.metric("Avg Fleet Signal", f"{avg_dbm_signal:.1f} dBm")
    
    st.markdown("---")


# --- MAIN ANALYTICS VIEW ---
tab1, tab2 = st.tabs(["📊 Diagnostic Analytics", "🗂️ Live Lakehouse Datasets"])

with tab1:
    col_left, col_right = st.columns([3, 2])
    
    with col_left:
        st.subheader("🌐 Fleet Profiling: Latency vs. Signal Strength")
        if not df_filtered_profile.empty:
            # High-end interactive scatter plot to isolate cluster anomalies immediately
            fig = px.scatter(
                df_filtered_profile,
                x="avg_dbm",
                y="avg_latency_ms",
                color="max_packet_loss_pct",
                size="avg_latency_ms",
                hover_data=["device_id", "hardware_alert_flag"],
                labels={"avg_dbm": "Signal Strength (dBm)", "avg_latency_ms": "Average Latency (ms)"},
                color_continuous_scale=px.colors.sequential.Cividis,
                template="plotly_dark"
            )
            fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Adjust filtering parameters to render diagnostic matrix visualization.")
            
    with col_right:
        st.subheader("🚨 Top High-Risk Incident Log Queue")
        if not df_filtered_detailed.empty:
            # Isolate anomalies that field techs need to know about
            anomalous_queue = df_filtered_detailed[df_filtered_detailed['packet_loss_pct'] > 2.0].sort_values(
                by=['packet_loss_pct', 'timestamp'], ascending=[False, False]
            )
            st.dataframe(
                anomalous_queue[['device_id', 'status_code', 'packet_loss_pct', 'signal_strength_dbm']].head(12),
                use_container_width=True,
                height=400
            )
        else:
            st.success("All device nodes operating within standard specifications.")

with tab2:
    st.subheader("💾 Raw Query Interface (Silver to Gold Inspection)")
    st.markdown("Inspect data exactly as it is represented inside your persistent storage tables.")
    
    exp1 = st.expander("View Gold Summarized Device Profiles Table")
    exp1.dataframe(df_filtered_profile, use_container_width=True)
    
    exp2 = st.expander("View Partitioned Detailed Telemetry Stream")
    exp2.dataframe(df_filtered_detailed.head(100), use_container_width=True)