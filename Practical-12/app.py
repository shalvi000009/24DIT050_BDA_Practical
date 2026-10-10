"""
========================================================================================
BDA Practical 12: Interactive Customer Sales Dashboard
Scenario        : Retail Analytics Executive Dashboard
Framework       : Streamlit + Plotly + Pandas
Description     : Interactive web application providing real-time multi-dimensional 
                  filtering, KPI monitoring, drill-down exploration, product 
                  profitability, regional insights, and customer segmentation.
Run locally     : streamlit run app.py
========================================================================================
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# -----------------------------------------------------------------------------
# PAGE CONFIGURATION & STYLING
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Retail Analytics Executive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Executive CSS
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .metric-card {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 16px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
        border: 1px solid #e9ecef;
        text-align: center;
    }
    .metric-title {
        font-size: 13px;
        color: #6c757d;
        font-weight: 600;
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 22px;
        font-weight: 700;
        color: #1a252f;
    }
    .metric-sub {
        font-size: 12px;
        color: #28a745;
        font-weight: 500;
    }
    .metric-sub-neg {
        font-size: 12px;
        color: #dc3545;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# DATA LOADING & CACHING
# -----------------------------------------------------------------------------
@st.cache_data
def load_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "output", "cleaned_sales.csv")
    
    if not os.path.exists(csv_path):
        csv_path = os.path.join(base_dir, "data", "superstore_sales.csv")
        
    df = pd.read_csv(csv_path)
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])
    df['Year'] = df['Order Date'].dt.year
    df['Quarter'] = df['Order Date'].dt.to_period('Q').astype(str)
    df['YearMonth'] = df['Order Date'].dt.to_period('M').astype(str)
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"Error loading dataset: {e}. Please ensure 'python data_processing.py' has been executed.")
    st.stop()

# -----------------------------------------------------------------------------
# SIDEBAR CONTROLS & INTERACTIVE FILTERS
# -----------------------------------------------------------------------------
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=60)
st.sidebar.title("🎛️ Dashboard Controls")
st.sidebar.markdown("**Interactive Multi-Dimensional Filters**")

# 1. Region Filter
all_regions = ["All Regions"] + sorted(list(df['Region'].unique()))
selected_region = st.sidebar.selectbox("Geographic Region:", all_regions)

# 2. Category Filter
all_categories = ["All Categories"] + sorted(list(df['Category'].unique()))
selected_category = st.sidebar.selectbox("Product Category:", all_categories)

# 3. Customer Segment Filter
all_segments = ["All Segments"] + sorted(list(df['Segment'].unique()))
selected_segment = st.sidebar.selectbox("Customer Segment:", all_segments)

# 4. Multi-Select Year Filter
all_years = sorted(list(df['Year'].unique()))
selected_years = st.sidebar.multiselect("Select Years:", all_years, default=all_years)

# Apply Active Filters
filtered_df = df.copy()
if selected_region != "All Regions":
    filtered_df = filtered_df[filtered_df['Region'] == selected_region]
if selected_category != "All Categories":
    filtered_df = filtered_df[filtered_df['Category'] == selected_category]
if selected_segment != "All Segments":
    filtered_df = filtered_df[filtered_df['Segment'] == selected_segment]
if selected_years:
    filtered_df = filtered_df[filtered_df['Year'].isin(selected_years)]

# -----------------------------------------------------------------------------
# DASHBOARD HEADER
# -----------------------------------------------------------------------------
st.title("📊 Retail Analytics Executive Sales Dashboard")
st.markdown("Real-time interactive monitoring of sales performance, product profitability, regional distribution, and customer segmentation.")

# Active Filter Status Banner
filter_info = []
if selected_region != "All Regions": filter_info.append(f"Region: **{selected_region}**")
if selected_category != "All Categories": filter_info.append(f"Category: **{selected_category}**")
if selected_segment != "All Segments": filter_info.append(f"Segment: **{selected_segment}**")
if len(selected_years) < len(all_years): filter_info.append(f"Years: **{', '.join(map(str, selected_years))}**")

if filter_info:
    st.info("Active Filters ➔ " + " | ".join(filter_info))
else:
    st.caption("Showing all records (No active filters applied). Use the sidebar to filter data.")

st.markdown("---")

# -----------------------------------------------------------------------------
# TOP ROW: 7 EXECUTIVE KPI CARDS
# -----------------------------------------------------------------------------
tot_sales = filtered_df['Sales'].sum()
tot_profit = filtered_df['Profit'].sum()
tot_orders = filtered_df['Order ID'].nunique()
tot_cust = filtered_df['Customer ID'].nunique()
tot_items = filtered_df['Quantity'].sum()
aov = tot_sales / tot_orders if tot_orders > 0 else 0.0
profit_margin = (tot_profit / tot_sales * 100) if tot_sales > 0 else 0.0

col1, col2, col3, col4, col5, col6, col7 = st.columns(7)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total Sales</div>
        <div class="metric-value">${tot_sales:,.0f}</div>
        <div class="metric-sub">Revenue</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    profit_color = "metric-sub" if tot_profit >= 0 else "metric-sub-neg"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total Profit</div>
        <div class="metric-value">${tot_profit:,.0f}</div>
        <div class="{profit_color}">Net Return</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Distinct Orders</div>
        <div class="metric-value">{tot_orders:,}</div>
        <div class="metric-sub">Transactions</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total Customers</div>
        <div class="metric-value">{tot_cust:,}</div>
        <div class="metric-sub">Active Accounts</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Units Sold</div>
        <div class="metric-value">{tot_items:,}</div>
        <div class="metric-sub">Quantity</div>
    </div>
    """, unsafe_allow_html=True)

with col6:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Avg Order Value</div>
        <div class="metric-value">${aov:,.2f}</div>
        <div class="metric-sub">Sales/Order</div>
    </div>
    """, unsafe_allow_html=True)

with col7:
    pm_color = "metric-sub" if profit_margin >= 0 else "metric-sub-neg"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Profit Margin</div>
        <div class="metric-value">{profit_margin:.1f}%</div>
        <div class="{pm_color}">Net Margin</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# NAVIGATION TABS
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📌 Executive Overview",
    "📦 Product & Category Analysis",
    "🗺️ Regional & State Explorer",
    "👥 Customer Segmentation",
    "📋 Transaction Explorer"
])

# =============================================================================
# TAB 1: EXECUTIVE OVERVIEW
# =============================================================================
with tab1:
    st.subheader("Monthly Revenue & Profitability Trends")
    
    # Monthly aggregation
    monthly_df = filtered_df.groupby('YearMonth').agg(
        Monthly_Sales=('Sales', 'sum'),
        Monthly_Profit=('Profit', 'sum')
    ).reset_index()

    fig_monthly = go.Figure()
    fig_monthly.add_trace(go.Scatter(
        x=monthly_df['YearMonth'], y=monthly_df['Monthly_Sales'],
        mode='lines+markers', name='Sales ($)',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=6)
    ))
    fig_monthly.add_trace(go.Bar(
        x=monthly_df['YearMonth'], y=monthly_df['Monthly_Profit'],
        name='Profit ($)',
        marker_color=np.where(monthly_df['Monthly_Profit'] >= 0, '#2ca02c', '#d62728'),
        opacity=0.75
    ))

    fig_monthly.update_layout(
        title="Monthly Sales vs Net Profit Over Time",
        xaxis_title="Year-Month",
        yaxis_title="Amount ($)",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=380
    )
    st.plotly_chart(fig_monthly, use_container_width=True)

    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Sales & Profit by Geographic Region")
        reg_df = filtered_df.groupby('Region').agg(
            Sales=('Sales', 'sum'),
            Profit=('Profit', 'sum')
        ).reset_index()

        fig_reg = go.Figure(data=[
            go.Bar(name='Sales', x=reg_df['Region'], y=reg_df['Sales'], marker_color='#1f77b4'),
            go.Bar(name='Profit', x=reg_df['Region'], y=reg_df['Profit'], marker_color='#2ca02c')
        ])
        fig_reg.update_layout(
            barmode='group',
            title="Regional Performance Comparison",
            xaxis_title="Region",
            yaxis_title="Amount ($)",
            template="plotly_white",
            height=340
        )
        st.plotly_chart(fig_reg, use_container_width=True)

    with col_b:
        st.subheader("Quarterly Performance Comparison")
        qtr_df = filtered_df.groupby('Quarter').agg(
            Sales=('Sales', 'sum'),
            Profit=('Profit', 'sum')
        ).reset_index()

        fig_qtr = go.Figure(data=[
            go.Bar(name='Sales', x=qtr_df['Quarter'], y=qtr_df['Sales'], marker_color='#3366cc'),
            go.Bar(name='Profit', x=qtr_df['Quarter'], y=qtr_df['Profit'], marker_color='#109618')
        ])
        fig_qtr.update_layout(
            barmode='group',
            title="Quarterly Sales vs Profit Comparison",
            xaxis_title="Quarter",
            yaxis_title="Amount ($)",
            template="plotly_white",
            height=340
        )
        st.plotly_chart(fig_qtr, use_container_width=True)

# =============================================================================
# TAB 2: PRODUCT & CATEGORY ANALYSIS
# =============================================================================
with tab2:
    st.subheader("Product Category & Sub-Category Breakdown")
    
    cat_df = filtered_df.groupby(['Category', 'Sub-Category']).agg(
        Total_Sales=('Sales', 'sum'),
        Total_Profit=('Profit', 'sum'),
        Quantity_Sold=('Quantity', 'sum'),
        Distinct_Orders=('Order ID', 'nunique')
    ).reset_index()
    cat_df['Profit_Margin_%'] = (cat_df['Total_Profit'] / cat_df['Total_Sales'] * 100).round(2)

    fig_cat_sub = px.bar(
        cat_df, x='Sub-Category', y='Total_Sales', color='Category',
        title="Sales Volume by Sub-Category",
        text_auto='.2s',
        template="plotly_white",
        height=380
    )
    st.plotly_chart(fig_cat_sub, use_container_width=True)

    col_p1, col_p2 = st.columns(2)
    
    with col_p1:
        st.markdown("#### 🏆 Top 10 Products by Sales Volume")
        top_sales = filtered_df.groupby('Product Name')['Sales'].sum().reset_index()
        top_sales = top_sales.sort_values(by='Sales', ascending=True).tail(10)
        
        fig_top_sales = px.bar(
            top_sales, x='Sales', y='Product Name', orientation='h',
            text_auto='.2s', color_discrete_sequence=['#1f77b4'],
            template="plotly_white", height=360
        )
        st.plotly_chart(fig_top_sales, use_container_width=True)

    with col_p2:
        st.markdown("#### 💰 Top 10 Products by Net Profit")
        top_profit = filtered_df.groupby('Product Name')['Profit'].sum().reset_index()
        top_profit = top_profit.sort_values(by='Profit', ascending=True).tail(10)
        
        fig_top_profit = px.bar(
            top_profit, x='Profit', y='Product Name', orientation='h',
            text_auto='.2s', color_discrete_sequence=['#2ca02c'],
            template="plotly_white", height=360
        )
        st.plotly_chart(fig_top_profit, use_container_width=True)

    # Loss Leader Products Alert Table
    st.markdown("---")
    st.markdown("### ⚠️ Loss-Leader & Negative Profit Products")
    st.caption("Products generating revenue but suffering net profit losses due to high discounting (>20%).")

    loss_products = filtered_df.groupby(['Product Name', 'Category', 'Sub-Category']).agg(
        Total_Sales=('Sales', 'sum'),
        Total_Profit=('Profit', 'sum'),
        Avg_Discount=('Discount', 'mean'),
        Total_Orders=('Order ID', 'nunique')
    ).reset_index()
    
    loss_products = loss_products[loss_products['Total_Profit'] < 0].sort_values(by='Total_Profit', ascending=True)

    if not loss_products.empty:
        st.dataframe(
            loss_products.style.format({
                "Total_Sales": "${:,.2f}",
                "Total_Profit": "${:,.2f}",
                "Avg_Discount": "{:.1%}"
            }),
            use_container_width=True
        )
    else:
        st.success("No negative-profit products found under the active filter selection!")

# =============================================================================
# TAB 3: REGIONAL & STATE EXPLORER
# =============================================================================
with tab3:
    st.subheader("Geographical Distribution & State-Level Analysis")
    
    state_df = filtered_df.groupby(['Region', 'State']).agg(
        Sales=('Sales', 'sum'),
        Profit=('Profit', 'sum'),
        Orders=('Order ID', 'nunique')
    ).reset_index()
    state_df['Profit_Margin_%'] = (state_df['Profit'] / state_df['Sales'] * 100).round(2)
    state_df = state_df.sort_values(by='Sales', ascending=False)

    col_r1, col_r2 = st.columns([3, 2])

    with col_r1:
        fig_state = px.bar(
            state_df, x='State', y='Sales', color='Region',
            title="Sales Volume by US State",
            template="plotly_white",
            height=400
        )
        st.plotly_chart(fig_state, use_container_width=True)

    with col_r2:
        st.markdown("#### State Performance Metrics Table")
        st.dataframe(
            state_df[['State', 'Sales', 'Profit', 'Profit_Margin_%']].style.format({
                "Sales": "${:,.2f}",
                "Profit": "${:,.2f}",
                "Profit_Margin_%": "{:.2f}%"
            }),
            height=360,
            use_container_width=True
        )

# =============================================================================
# TAB 4: CUSTOMER SEGMENTATION
# =============================================================================
with tab4:
    st.subheader("Customer Segment Performance")
    
    seg_df = filtered_df.groupby('Segment').agg(
        Total_Sales=('Sales', 'sum'),
        Total_Profit=('Profit', 'sum'),
        Distinct_Customers=('Customer ID', 'nunique'),
        Distinct_Orders=('Order ID', 'nunique')
    ).reset_index()
    seg_df['AOV'] = (seg_df['Total_Sales'] / seg_df['Distinct_Orders']).round(2)

    col_s1, col_s2 = st.columns(2)

    with col_s1:
        fig_seg_sales = px.pie(
            seg_df, values='Total_Sales', names='Segment',
            title="Sales Contribution by Customer Segment",
            hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig_seg_sales, use_container_width=True)

    with col_s2:
        fig_seg_aov = px.bar(
            seg_df, x='Segment', y='AOV', color='Segment',
            title="Average Order Value (AOV) by Segment ($)",
            text_auto='.2f', template="plotly_white"
        )
        st.plotly_chart(fig_seg_aov, use_container_width=True)

    st.markdown("---")
    st.markdown("### 👑 High-Value Customer Leaderboard")
    
    top_customers = filtered_df.groupby(['Customer ID', 'Customer Name', 'Segment']).agg(
        Lifetime_Spend=('Sales', 'sum'),
        Net_Profit=('Profit', 'sum'),
        Total_Orders=('Order ID', 'nunique'),
        Avg_Order_Value=('Sales', 'mean')
    ).reset_index().sort_values(by='Lifetime_Spend', ascending=False).head(15)

    st.dataframe(
        top_customers.style.format({
            "Lifetime_Spend": "${:,.2f}",
            "Net_Profit": "${:,.2f}",
            "Avg_Order_Value": "${:,.2f}"
        }),
        use_container_width=True
    )

# =============================================================================
# TAB 5: TRANSACTION EXPLORER & DATASET SEARCH
# =============================================================================
with tab5:
    st.subheader("Filterable Raw Order Transactions Table")
    
    search_query = st.text_input("🔍 Search orders by Customer Name, Product Name, Order ID, or City:")
    
    table_df = filtered_df.copy()
    if search_query:
        mask = (
            table_df['Customer Name'].str.contains(search_query, case=False, na=False) |
            table_df['Product Name'].str.contains(search_query, case=False, na=False) |
            table_df['Order ID'].str.contains(search_query, case=False, na=False) |
            table_df['City'].str.contains(search_query, case=False, na=False)
        )
        table_df = table_df[mask]

    st.caption(f"Displaying {len(table_df):,} matching order records")
    
    display_cols = [
        "Order ID", "Order Date", "Customer Name", "Segment", "Region", "State",
        "Category", "Sub-Category", "Product Name", "Sales", "Quantity", "Discount", "Profit"
    ]

    st.dataframe(
        table_df[display_cols].style.format({
            "Sales": "${:,.2f}",
            "Profit": "${:,.2f}",
            "Discount": "{:.0%}"
        }),
        height=450,
        use_container_width=True
    )

    # CSV Download Button
    csv_data = table_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Filtered Dataset CSV",
        data=csv_data,
        file_name="filtered_superstore_sales.csv",
        mime="text/csv"
    )

# -----------------------------------------------------------------------------
# FOOTER
# -----------------------------------------------------------------------------
st.markdown("---")
st.caption("Retail Analytics Executive Dashboard | PySpark & Streamlit Interactive Analytics | Practical 12")
