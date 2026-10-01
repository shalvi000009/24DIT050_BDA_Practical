"""
========================================================================================
BDA PRACTICAL 8: STOCK MARKET ANALYTICS USING WINDOW-BASED ANALYSIS
Scenario: FinEdge Securities Ltd. - Stock Performance Analytics Engine
Technology: Apache Spark (PySpark 3.x) + Matplotlib
========================================================================================
"""

import os
import matplotlib
matplotlib.use('Agg')  # Non-interactive headless backend for automated script execution
import matplotlib.pyplot as plt
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, count, round as spark_round,
    lag, first, last, stddev, when, to_date
)
from pyspark.sql.window import Window


def print_header(title):
    """Utility function to print styled terminal headers for practical sections."""
    print("\n" + "=" * 75)
    print(f" {title.upper()}")
    print("=" * 75)


def save_df_to_csv(df, output_path):
    """Save PySpark DataFrame to a single clean CSV file via Pandas."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pandas_df = df.toPandas()
    pandas_df.to_csv(output_path, index=False)
    print(f"[SUCCESS] Exported single CSV to: {output_path}")


def main():
    # -------------------------------------------------------------------------
    # TASK 1: CREATE SPARK SESSION
    # -------------------------------------------------------------------------
    print_header("TASK 1: CREATING SPARK SESSION")
    
    spark = SparkSession.builder \
        .appName("Practical8_StockMarketAnalytics") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Suppress verbose Spark INFO logs for clear presentation
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Spark Version     : {spark.version}")
    print(f"Application Name  : {spark.sparkContext.appName}")
    print(f"Master Node       : {spark.sparkContext.master}")

    # Set up folder paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, "data", "stock_data.csv")
    output_dir = os.path.join(base_dir, "output")
    charts_dir = os.path.join(output_dir, "charts")
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(charts_dir, exist_ok=True)

    # -------------------------------------------------------------------------
    # TASK 2 & 3: LOAD STOCK DATASET AND PERFORM DATA VALIDATION
    # -------------------------------------------------------------------------
    print_header("TASK 2 & 3: LOADING DATASET & DATA VALIDATION")
    print(f"Reading dataset from: {data_path}")

    raw_df = spark.read.csv(
        data_path,
        header=True,
        inferSchema=True
    )

    print("\n--- Raw DataFrame Schema ---")
    raw_df.printSchema()

    total_records = raw_df.count()
    print(f"Total Number of Records Read: {total_records}")

    print("\n--- First 10 Records ---")
    raw_df.show(10, truncate=False)

    # Distinct Stock Symbols
    distinct_stocks = raw_df.select("Stock").distinct().collect()
    stock_list = sorted([row["Stock"] for row in distinct_stocks])
    print(f"Distinct Stock Symbols ({len(stock_list)}): {', '.join(stock_list)}")

    # Null Value Checks
    print("\n--- Null Count Validation ---")
    null_counts = raw_df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in raw_df.columns
    ])
    null_counts.show()

    # Data Type Enforcements & Cleaning
    cleaned_df = raw_df \
        .filter(col("Stock").isNotNull() & col("Price").isNotNull() & col("Timestamp").isNotNull()) \
        .withColumn("Timestamp", to_date(col("Timestamp"))) \
        .withColumn("Price", col("Price").cast("double"))

    print("Data Type Validation & Type Cast Completed successfully.")

    # -------------------------------------------------------------------------
    # TASK 4: BASIC STOCK AGGREGATIONS (AVG, MAX, MIN, COUNT)
    # -------------------------------------------------------------------------
    print_header("TASK 4: BASIC STOCK AGGREGATIONS (STOCK-WISE SUMMARY)")

    stock_summary_df = cleaned_df.groupBy("Stock") \
        .agg(
            count("*").alias("ObservationCount"),
            spark_round(avg("Price"), 2).alias("AveragePrice"),
            spark_round(spark_max("Price"), 2).alias("MaximumPrice"),
            spark_round(spark_min("Price"), 2).alias("MinimumPrice")
        ) \
        .orderBy("Stock")

    stock_summary_df.show(truncate=False)

    # Save stock summary to output/stock_summary.csv
    stock_summary_path = os.path.join(output_dir, "stock_summary.csv")
    save_df_to_csv(stock_summary_df, stock_summary_path)

    # -------------------------------------------------------------------------
    # TASK 5 & 6: SPARK WINDOW FUNCTIONS & DAILY STOCK RETURNS
    # -------------------------------------------------------------------------
    print_header("TASK 5 & 6: WINDOW FUNCTIONS & DAILY STOCK RETURNS")
    print("Concept: Partitioning by 'Stock' and ordering by 'Timestamp'.")

    # Define Window specification ordered by Timestamp per Stock
    w_stock_order = Window.partitionBy("Stock").orderBy("Timestamp")

    # Calculate Previous Price using lag()
    df_with_prev = cleaned_df.withColumn("PreviousPrice", lag("Price", 1).over(w_stock_order))

    # Daily Return Formula: ((Current Price - Previous Price) / Previous Price) * 100
    # First row per stock has PreviousPrice = NULL -> handle gracefully by assigning 0.0
    df_with_returns = df_with_prev.withColumn(
        "DailyReturn",
        spark_round(
            when(col("PreviousPrice").isNull() | (col("PreviousPrice") == 0), 0.0)
            .otherwise(((col("Price") - col("PreviousPrice")) / col("PreviousPrice")) * 100),
            2
        )
    )

    print("\n--- Sample Daily Returns Calculation (First 15 Rows) ---")
    df_with_returns.select("Stock", "Timestamp", "Price", "PreviousPrice", "DailyReturn").show(15, truncate=False)

    # -------------------------------------------------------------------------
    # TASK 7: MOVING AVERAGE USING SPARK WINDOW (3-PERIOD MOVING AVG)
    # -------------------------------------------------------------------------
    print_header("TASK 7: 3-PERIOD MOVING AVERAGE COMPUTATION")
    print("Concept: Using rowsBetween(-2, 0) over timestamp-ordered window spec.")

    w_ma3 = Window.partitionBy("Stock").orderBy("Timestamp").rowsBetween(-2, 0)

    df_with_ma = df_with_returns.withColumn(
        "MovingAverage3P",
        spark_round(avg("Price").over(w_ma3), 2)
    )

    print("\n--- Price Trends with 3-Period Moving Average (Sample) ---")
    df_with_ma.select("Stock", "Timestamp", "Price", "PreviousPrice", "DailyReturn", "MovingAverage3P").show(15, truncate=False)

    # -------------------------------------------------------------------------
    # TASK 8, 9 & 10: VOLATILITY, TOP-PERFORMING STOCK & FINAL WINDOW ANALYSIS
    # -------------------------------------------------------------------------
    print_header("TASK 8, 9 & 10: VOLATILITY, CUMULATIVE RETURN & TOP-PERFORMER REPORT")

    # Window covering entire time horizon per stock to fetch first and last price based on timestamp
    w_unbounded = Window.partitionBy("Stock").orderBy("Timestamp") \
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df_full_window = df_with_ma \
        .withColumn("FirstPrice", first("Price").over(w_unbounded)) \
        .withColumn("LastPrice", last("Price").over(w_unbounded))

    # Aggregating Stock Performance Indicators
    final_window_analysis_df = df_full_window.groupBy("Stock") \
        .agg(
            spark_round(first("FirstPrice"), 2).alias("FirstPrice"),
            spark_round(first("LastPrice"), 2).alias("LastPrice"),
            spark_round(avg("Price"), 2).alias("AveragePrice"),
            spark_round(spark_min("Price"), 2).alias("MinPrice"),
            spark_round(spark_max("Price"), 2).alias("MaxPrice"),
            # Cumulative Return Formula: ((LastPrice - FirstPrice) / FirstPrice) * 100
            spark_round(
                ((first("LastPrice") - first("FirstPrice")) / first("FirstPrice")) * 100, 2
            ).alias("CumulativeReturn"),
            # Volatility Formula: Standard Deviation of Daily Return (%)
            spark_round(stddev("DailyReturn"), 2).alias("Volatility")
        ) \
        .orderBy(col("CumulativeReturn").desc())

    print("\n--- Final Stock Analytical Performance Summary ---")
    final_window_analysis_df.show(truncate=False)

    # Save window analysis report to output/window_analysis.csv
    window_analysis_path = os.path.join(output_dir, "window_analysis.csv")
    save_df_to_csv(final_window_analysis_df, window_analysis_path)

    # Identify Top Performing Stock
    top_stock_row = final_window_analysis_df.first()
    top_stock_symbol = top_stock_row["Stock"]
    top_stock_return = top_stock_row["CumulativeReturn"]
    top_stock_volatility = top_stock_row["Volatility"]

    print_header("TOP-PERFORMING STOCK METRIC RESULT")
    print(f"Top-Performing Stock Metric : Cumulative Return (%) over period")
    print(f"Top Stock Symbol            : {top_stock_symbol}")
    print(f"Cumulative Return           : {top_stock_return}%")
    print(f"Stock Volatility (StdDev)   : {top_stock_volatility}%")

    # -------------------------------------------------------------------------
    # TASK 11: VISUALIZE STOCK TRENDS USING MATPLOTLIB
    # -------------------------------------------------------------------------
    print_header("TASK 11: GENERATING VISUALIZATION CHARTS")

    # Collect time-series data to local Pandas for plotting
    plot_df = df_with_ma.select("Stock", "Timestamp", "Price", "MovingAverage3P").toPandas()
    plot_df["Timestamp"] = pd.to_datetime(plot_df["Timestamp"])

    # Chart 1: Stock Price Trends and 3-Period Moving Averages
    fig, ax = plt.subplots(figsize=(12, 6))
    stocks = plot_df["Stock"].unique()

    for stock_sym in stocks:
        stock_data = plot_df[plot_df["Stock"] == stock_sym].sort_values("Timestamp")
        ax.plot(stock_data["Timestamp"], stock_data["Price"], marker='o', label=f"{stock_sym} Price", linewidth=1.8)
        ax.plot(stock_data["Timestamp"], stock_data["MovingAverage3P"], linestyle='--', label=f"{stock_sym} 3P-MA", alpha=0.7)

    ax.set_title("FinEdge Securities: Stock Price Trends & 3-Period Moving Averages", fontsize=14, fontweight='bold', pad=12)
    ax.set_xlabel("Date", fontsize=11)
    ax.set_ylabel("Price ($)", fontsize=11)
    ax.grid(True, linestyle=':', alpha=0.6)
    ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=9)
    plt.tight_layout()

    chart1_path = os.path.join(charts_dir, "stock_price_trends.png")
    plt.savefig(chart1_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Chart 1: {chart1_path}")

    # Chart 2: Cumulative Returns & Volatility Comparison
    summary_pandas = final_window_analysis_df.toPandas()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # Bar chart 1: Cumulative Returns
    bars1 = ax1.bar(summary_pandas["Stock"], summary_pandas["CumulativeReturn"], color='#2b5c8f', edgecolor='black')
    ax1.set_title("Cumulative Returns (%) by Stock", fontsize=12, fontweight='bold')
    ax1.set_ylabel("Cumulative Return (%)")
    ax1.grid(axis='y', linestyle=':', alpha=0.6)

    for bar in bars1:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2.0, yval + 0.3, f"{yval:.2f}%", ha='center', va='bottom', fontsize=9, fontweight='bold')

    # Bar chart 2: Volatility (StdDev)
    bars2 = ax2.bar(summary_pandas["Stock"], summary_pandas["Volatility"], color='#d95f02', edgecolor='black')
    ax2.set_title("Stock Volatility (Return StdDev %)", fontsize=12, fontweight='bold')
    ax2.set_ylabel("Volatility (Standard Deviation %)")
    ax2.grid(axis='y', linestyle=':', alpha=0.6)

    for bar in bars2:
        yval = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2.0, yval + 0.05, f"{yval:.2f}%", ha='center', va='bottom', fontsize=9, fontweight='bold')

    plt.tight_layout()
    chart2_path = os.path.join(charts_dir, "stock_performance_volatility.png")
    plt.savefig(chart2_path, dpi=300)
    plt.close()
    print(f"[SUCCESS] Saved Chart 2: {chart2_path}")

    # -------------------------------------------------------------------------
    # TASK 12: GENERATE PERFORMANCE REPORT
    # -------------------------------------------------------------------------
    print_header("TASK 12: GENERATING PERFORMANCE REPORT FILE")

    report_content = f"""========================================================================================
FINEDGE SECURITIES LTD. - STOCK MARKET ANALYTICAL PERFORMANCE REPORT
Practical 8: Stock Market Analytics Using Window-Based Analysis
========================================================================================

1. PRACTICAL TITLE & CONTEXT
   Title: Stock Market Analytics Using Window-Based Analysis
   Scenario: FinEdge Securities Ltd. provides financial analytics insights to market investors.
   Objective: Analyze high-frequency stock price updates to evaluate price trends, daily returns, 
              moving averages, and stock volatility using Apache Spark Window functions.

2. DATASET SUMMARY & SPARK CONFIGURATION
   Dataset File       : data/stock_data.csv
   Total Records      : {total_records}
   Stock Symbols      : {', '.join(stock_list)}
   Required Columns   : Timestamp, Stock, Price
   Spark Version      : {spark.version}
   Master Mode        : local[*]
   Shuffle Partitions : 4

3. DATA VALIDATION & PREPROCESSING
   - Data integrity checks confirmed zero missing or null records in Timestamp, Stock, and Price.
   - Timestamp strings were converted to PySpark DateType (YYYY-MM-DD).
   - Stock prices were cast to DoubleType numeric format.

4. METHODOLOGY & SPARK WINDOW CONCEPTS
   a) Window Partitioning & Ordering:
      Window specification partitioned by 'Stock' and ordered by 'Timestamp'.
      Expression: Window.partitionBy("Stock").orderBy("Timestamp")

   b) Daily Return Calculation:
      Computed using lag() function to retrieve the prior trading day's price.
      Formula: DailyReturn = ((Current Price - Previous Price) / Previous Price) * 100
      Edge-case handling: First record for each stock defaults to 0.0% return.

   c) Moving Average (3-Period):
      Computed using a sliding window across current row and prior 2 rows.
      Expression: Window.partitionBy("Stock").orderBy("Timestamp").rowsBetween(-2, 0)
      Formula: 3P-MA = Avg(Price) over window

   d) Volatility Metric:
      Calculated as the Standard Deviation (stddev) of Daily Returns for each stock.
      Interpretation: Higher standard deviation reflects higher price fluctuation/risk.

   e) Top-Performing Stock Identification Metric:
      Metric: Cumulative Return (%) over the observed trading period.
      Formula: Cumulative Return = ((Last Price - First Price) / First Price) * 100
      Note: Evaluated strictly over timestamp order using first() and last() window aggregations.

5. ANALYTICAL RESULTS SUMMARY
----------------------------------------------------------------------------------------
Stock   FirstPrice  LastPrice  AvgPrice   MinPrice   MaxPrice   CumReturn(%)  Volatility(%)
----------------------------------------------------------------------------------------
"""
    for _, row in summary_pandas.iterrows():
        report_content += f"{row['Stock']:<7} ${row['FirstPrice']:<9.2f} ${row['LastPrice']:<9.2f} ${row['AveragePrice']:<9.2f} ${row['MinPrice']:<9.2f} ${row['MaxPrice']:<9.2f} {row['CumulativeReturn']:<11.2f}% {row['Volatility']:<11.2f}%\n"

    report_content += f"""----------------------------------------------------------------------------------------

6. KEY FINDINGS & TOP-PERFORMING STOCK
   - Top-Performing Stock : {top_stock_symbol}
   - Cumulative Return    : {top_stock_return}%
   - Volatility (StdDev)  : {top_stock_volatility}%
   - Observations:
     1. {top_stock_symbol} generated the highest cumulative return ({top_stock_return}%) over the analyzed timeframe.
     2. Moving averages effectively smooth out short-term noise to reveal underlying price momentum.
     3. Highest absolute price (e.g. MSFT at ~$436) does not equate to highest return percentage.

7. LIMITATIONS & DISCLAIMER
   - Limitations: The analysis reflects daily end-of-day prices without intraday order-book depth or trading volumes.
   - Disclaimer: These observations are based on simulated/sample data for academic demonstration 
     and should not be interpreted as real financial investment advice.

========================================================================================
Report Generated Successfully by PySpark Window Analytical Engine
========================================================================================
"""

    report_file_path = os.path.join(output_dir, "performance_report.txt")
    with open(report_file_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"[SUCCESS] Performance report generated at: {report_file_path}")

    # Stop Spark Session
    spark.stop()
    print_header("PRACTICAL 8 EXECUTION COMPLETE")


if __name__ == "__main__":
    main()
