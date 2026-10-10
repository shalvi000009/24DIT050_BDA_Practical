"""
========================================================================================
BDA Practical 12: Data Cleaning, Validation, and Metric Summary Engine
Scenario        : Retail Analytics Executive Dashboard
Description     : Validates, cleans, and pre-processes Superstore sales data.
                  Computes multi-dimensional KPI summaries and exports CSV artifacts.
========================================================================================
"""

import os
import pandas as pd
import numpy as np

def load_and_validate_sales_data(input_path="data/superstore_sales.csv", output_dir="output"):
    """
    Loads raw sales dataset, runs validation checks, cleans invalid data,
    saves output/cleaned_sales.csv and output/data_quality_report.txt.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found at '{input_path}'. Run 'python generate_dataset.py' first.")

    os.makedirs(output_dir, exist_ok=True)
    df_raw = pd.read_csv(input_path)
    raw_row_count = len(df_raw)

    report_lines = []
    report_lines.append("========================================================================================")
    report_lines.append("SUPERSTORE SALES DATA QUALITY & VALIDATION REPORT")
    report_lines.append("========================================================================================")
    report_lines.append(f"Input Dataset Path   : {os.path.abspath(input_path)}")
    report_lines.append(f"Initial Record Count : {raw_row_count:,}\n")

    # 1. Missing Values Check
    report_lines.append("--- 1. MISSING VALUE ANALYSIS ---")
    missing_series = df_raw.isnull().sum()
    total_missing = missing_series.sum()
    if total_missing == 0:
        report_lines.append("✔ Zero missing values found across all columns.")
    else:
        for col, count in missing_series[missing_series > 0].items():
            report_lines.append(f"  - Column '{col}': {count} missing values")

    # 2. Duplicate Rows Check
    report_lines.append("\n--- 2. DUPLICATE RECORD ANALYSIS ---")
    duplicates_count = df_raw.duplicated().sum()
    report_lines.append(f"Duplicate rows detected: {duplicates_count}")
    df_clean = df_raw.drop_duplicates()

    # 3. Date Parsing & Validation
    report_lines.append("\n--- 3. DATE VALIDATION & FEATURE ENGINEERING ---")
    df_clean['Order Date'] = pd.to_datetime(df_clean['Order Date'], errors='coerce')
    df_clean['Ship Date'] = pd.to_datetime(df_clean['Ship Date'], errors='coerce')

    invalid_order_dates = df_clean['Order Date'].isnull().sum()
    invalid_ship_dates = df_clean['Ship Date'].isnull().sum()
    report_lines.append(f"Invalid Order Dates : {invalid_order_dates}")
    report_lines.append(f"Invalid Ship Dates  : {invalid_ship_dates}")

    # Drop unparseable dates if any
    df_clean = df_clean.dropna(subset=['Order Date', 'Ship Date'])

    # Feature Engineering for temporal analysis
    df_clean['Year'] = df_clean['Order Date'].dt.year
    df_clean['Quarter'] = df_clean['Order Date'].dt.to_period('Q').astype(str)
    df_clean['YearMonth'] = df_clean['Order Date'].dt.to_period('M').astype(str)
    df_clean['MonthName'] = df_clean['Order Date'].dt.strftime('%b')
    df_clean['DayOfWeek'] = df_clean['Order Date'].dt.strftime('%A')
    df_clean['ShippingDays'] = (df_clean['Ship Date'] - df_clean['Order Date']).dt.days

    # 4. Numerical Fields Validation
    report_lines.append("\n--- 4. NUMERICAL VALUE VALIDATION ---")
    invalid_sales = (df_clean['Sales'] <= 0).sum()
    invalid_quantity = (df_clean['Quantity'] <= 0).sum()
    invalid_discount = ((df_clean['Discount'] < 0) | (df_clean['Discount'] > 1.0)).sum()

    report_lines.append(f"Non-positive Sales rows    : {invalid_sales}")
    report_lines.append(f"Non-positive Quantity rows : {invalid_quantity}")
    report_lines.append(f"Invalid Discount values    : {invalid_discount}")

    df_clean = df_clean[(df_clean['Sales'] > 0) & (df_clean['Quantity'] > 0) & (df_clean['Discount'] >= 0)]

    # 5. Negative Profit Analysis (Retail Business Loss vs Dirty Data)
    negative_profit_rows = (df_clean['Profit'] < 0).sum()
    report_lines.append("\n--- 5. NEGATIVE PROFIT BUSINESS ANALYSIS ---")
    report_lines.append(f"Negative Profit Transactions : {negative_profit_rows:,} ({negative_profit_rows / len(df_clean) * 100:.1f}%)")
    report_lines.append("Note: Negative profit transactions are VALID retail orders representing loss-leader items")
    report_lines.append("      or heavily discounted products (e.g. Tables/Binders with 30%+ discount).")
    report_lines.append("      These records are preserved for accurate profitability analysis.")

    cleaned_row_count = len(df_clean)
    report_lines.append("\n========================================================================================")
    report_lines.append(f"FINAL CLEANED RECORD COUNT: {cleaned_row_count:,} (Dropped {raw_row_count - cleaned_row_count} rows)")
    report_lines.append("========================================================================================")

    # Save Data Quality Report
    report_path = os.path.join(output_dir, "data_quality_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    # Save Cleaned CSV
    cleaned_csv_path = os.path.join(output_dir, "cleaned_sales.csv")
    df_clean.to_csv(cleaned_csv_path, index=False)

    print(f"Data quality report saved to: {report_path}")
    print(f"Cleaned dataset saved to     : {cleaned_csv_path}")

    return df_clean

def compute_and_save_summaries(df, output_dir="output"):
    """
    Computes summary tables for overall KPIs, category, region, and quarterly breakdowns.
    Exports result CSV files to output/ directory.
    """
    os.makedirs(output_dir, exist_ok=True)

    # 1. Overall Sales Summary (sales_summary.csv)
    total_sales = df['Sales'].sum()
    total_profit = df['Profit'].sum()
    total_orders = df['Order ID'].nunique()
    total_customers = df['Customer ID'].nunique()
    total_items = df['Quantity'].sum()
    aov = total_sales / total_orders if total_orders > 0 else 0.0
    profit_margin = (total_profit / total_sales * 100) if total_sales > 0 else 0.0

    sales_summary_df = pd.DataFrame([{
        "Metric": "Total Sales ($)", "Value": f"${total_sales:,.2f}"},
        {"Metric": "Total Profit ($)", "Value": f"${total_profit:,.2f}"},
        {"Metric": "Distinct Orders (#)", "Value": f"{total_orders:,}"},
        {"Metric": "Distinct Customers (#)", "Value": f"{total_customers:,}"},
        {"Metric": "Total Quantity Sold (#)", "Value": f"{total_items:,}"},
        {"Metric": "Average Order Value ($)", "Value": f"${aov:,.2f}"},
        {"Metric": "Overall Profit Margin (%)", "Value": f"{profit_margin:.2f}%"}
    ])
    sales_summary_df.to_csv(os.path.join(output_dir, "sales_summary.csv"), index=False)

    # 2. Category Summary (category_summary.csv)
    cat_summary = df.groupby(["Category", "Sub-Category"]).agg(
        Total_Sales=("Sales", "sum"),
        Total_Profit=("Profit", "sum"),
        Total_Quantity=("Quantity", "sum"),
        Distinct_Orders=("Order ID", "nunique")
    ).reset_index()
    cat_summary["Profit_Margin_%"] = (cat_summary["Total_Profit"] / cat_summary["Total_Sales"] * 100).round(2)
    cat_summary.sort_values(by="Total_Sales", ascending=False, inplace=True)
    cat_summary.to_csv(os.path.join(output_dir, "category_summary.csv"), index=False)

    # 3. Region Summary (region_summary.csv)
    reg_summary = df.groupby(["Region", "State"]).agg(
        Total_Sales=("Sales", "sum"),
        Total_Profit=("Profit", "sum"),
        Total_Quantity=("Quantity", "sum"),
        Distinct_Orders=("Order ID", "nunique")
    ).reset_index()
    reg_summary["Profit_Margin_%"] = (reg_summary["Total_Profit"] / reg_summary["Total_Sales"] * 100).round(2)
    reg_summary.sort_values(by="Total_Sales", ascending=False, inplace=True)
    reg_summary.to_csv(os.path.join(output_dir, "region_summary.csv"), index=False)

    # 4. Quarterly Summary (quarterly_summary.csv)
    qtr_summary = df.groupby(["Year", "Quarter"]).agg(
        Total_Sales=("Sales", "sum"),
        Total_Profit=("Profit", "sum"),
        Total_Quantity=("Quantity", "sum"),
        Distinct_Orders=("Order ID", "nunique")
    ).reset_index()
    qtr_summary["Profit_Margin_%"] = (qtr_summary["Total_Profit"] / qtr_summary["Total_Sales"] * 100).round(2)
    qtr_summary.sort_values(by=["Year", "Quarter"], inplace=True)
    qtr_summary.to_csv(os.path.join(output_dir, "quarterly_summary.csv"), index=False)

    print("Multi-dimensional summaries successfully exported to output/ CSVs.")

if __name__ == "__main__":
    cleaned_df = load_and_validate_sales_data()
    compute_and_save_summaries(cleaned_df)
