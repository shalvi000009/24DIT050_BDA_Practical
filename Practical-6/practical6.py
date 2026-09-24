"""
CSUE301 - Big Data Analytics
Practical 6: E-Commerce Sales Analysis Using Spark DataFrames
Student ID: 24DIT050
"""

import os
import sys
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, sum as _sum, avg as _avg, desc, asc

# 1. Initialize SparkSession
spark = SparkSession.builder \
    .appName("E-Commerce Sales Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Suppress verbose Spark INFO logs for clean terminal output
spark.sparkContext.setLogLevel("ERROR")

# Setup paths
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, "sales_data.csv")
outputs_dir = os.path.join(script_dir, "outputs")
os.makedirs(outputs_dir, exist_ok=True)

def save_output(filename, content):
    filepath = os.path.join(outputs_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

# Define explicit schema to preserve OrderID as String (0001, 0002...)
custom_schema = StructType([
    StructField("OrderID", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", IntegerType(), True),
    StructField("City", StringType(), True)
])

# 2. Load Dataset into PySpark DataFrame
df = spark.read.csv(csv_path, header=True, schema=custom_schema)

# ===== ORIGINAL DATA =====
print("\n===== ORIGINAL DATA =====")
df.show(truncate=False)
save_output("01_original_data.txt", "===== ORIGINAL DATA =====\n" + df._jdf.showString(20, 20, False))

# ===== SCHEMA & DATA TYPES =====
print("===== SCHEMA =====")
df.printSchema()

print("\n===== DATA TYPES =====")
dtypes_list = []
for col_name, dtype in df.dtypes:
    line = f"Column: {col_name:<10} | Type: {dtype}"
    print(line)
    dtypes_list.append(line)

schema_buf = StringIO()
sys.stdout = schema_buf
df.printSchema()
sys.stdout = sys.__stdout__

schema_file_content = "===== SCHEMA =====\n" + schema_buf.getvalue() + "\n===== DATA TYPES =====\n" + "\n".join(dtypes_list) + "\n"
save_output("02_schema_and_data_types.txt", schema_file_content)

# ===== REVENUE CALCULATION =====
print("\n===== REVENUE CALCULATION =====")
df_revenue = df.withColumn("Revenue", col("Quantity") * col("Price"))
df_revenue.show(truncate=False)
save_output("03_revenue_calculation.txt", "===== REVENUE CALCULATION =====\n" + df_revenue._jdf.showString(20, 20, False))

# ===== CATEGORY-WISE REVENUE =====
print("===== CATEGORY-WISE REVENUE =====")
category_revenue = df_revenue.groupBy("Category") \
    .agg(_sum("Revenue").alias("Total_Revenue")) \
    .orderBy(desc("Total_Revenue"))
category_revenue.show(truncate=False)
save_output("04_category_revenue.txt", "===== CATEGORY-WISE REVENUE =====\n" + category_revenue._jdf.showString(20, 20, False))

# ===== TOP-SELLING PRODUCTS =====
print("===== TOP-SELLING PRODUCTS =====")
top_products = df_revenue.groupBy("Product") \
    .agg(_sum("Quantity").alias("Total_Quantity_Sold")) \
    .orderBy(desc("Total_Quantity_Sold"))
top_products.show(truncate=False)
save_output("05_top_selling_products.txt", "===== TOP-SELLING PRODUCTS =====\n" + top_products._jdf.showString(20, 20, False))

# ===== CITY-WISE REVENUE =====
print("===== CITY-WISE REVENUE =====")
city_revenue = df_revenue.groupBy("City") \
    .agg(_sum("Revenue").alias("Total_Revenue")) \
    .orderBy(desc("Total_Revenue"))
city_revenue.show(truncate=False)
save_output("06_city_revenue.txt", "===== CITY-WISE REVENUE =====\n" + city_revenue._jdf.showString(20, 20, False))

# ===== TOP 3 REVENUE-GENERATING CITIES =====
print("===== TOP 3 REVENUE-GENERATING CITIES =====")
top_3_cities = city_revenue.limit(3)
top_3_cities.show(truncate=False)
save_output("07_top_3_cities.txt", "===== TOP 3 REVENUE-GENERATING CITIES =====\n" + top_3_cities._jdf.showString(20, 20, False))

# ===== AVERAGE REVENUE PER ORDER =====
print("===== AVERAGE REVENUE PER ORDER =====")
avg_revenue_row = df_revenue.agg(_avg("Revenue").alias("Average_Revenue_Per_Order")).collect()[0]
avg_revenue_val = avg_revenue_row["Average_Revenue_Per_Order"]
print(f"Average Revenue Per Order: INR {avg_revenue_val:,.2f}")

save_output("08_average_revenue.txt", f"===== AVERAGE REVENUE PER ORDER =====\nAverage Revenue Per Order: INR {avg_revenue_val:,.2f}\n")

# ===== DISCOUNT ANALYSIS =====
print("\n===== DISCOUNT ANALYSIS =====")
df_discount = df_revenue.withColumn("Discount", col("Revenue") * 0.10) \
                        .withColumn("Final_Revenue", col("Revenue") - col("Discount"))
df_discount.show(truncate=False)
save_output("09_discount_analysis.txt", "===== DISCOUNT ANALYSIS =====\n" + df_discount._jdf.showString(20, 20, False))

# ===== LEAST-PERFORMING CATEGORY =====
print("===== LEAST-PERFORMING CATEGORY =====")
least_category = df_discount.groupBy("Category") \
    .agg(_sum("Final_Revenue").alias("Total_Final_Revenue")) \
    .orderBy(asc("Total_Final_Revenue"))
least_category.show(truncate=False)

lowest_cat_row = least_category.first()
least_cat_text = f"Least-Performing Category (After 10% Discount): {lowest_cat_row['Category']} with Final Revenue: INR {lowest_cat_row['Total_Final_Revenue']:,.2f}"
print(least_cat_text)

save_output("10_least_performing_category.txt", "===== LEAST-PERFORMING CATEGORY =====\n" + least_category._jdf.showString(20, 20, False) + f"\n{least_cat_text}\n")

# ===== MONTHLY REVENUE =====
print("\n===== MONTHLY REVENUE =====")
monthly_msg = ("Monthly revenue analysis requires an OrderDate column.\n"
               "The current dataset does not contain a date column.\n"
               "To perform monthly revenue analysis in PySpark when dates are available:\n"
               "  df_dated = df.withColumn('OrderDate', to_date(col('DateStr'), 'yyyy-MM-dd'))\n"
               "  monthly_rev = df_dated.groupBy(month('OrderDate').alias('Month')).agg(sum('Revenue'))")
print(monthly_msg)
save_output("11_monthly_revenue.txt", "===== MONTHLY REVENUE =====\n" + monthly_msg + "\n")

# ===== BUSINESS INSIGHTS =====
print("\n===== BUSINESS INSIGHTS =====")
top_cat_row = category_revenue.first()
top_prod_row = top_products.first()
top_city_row = city_revenue.first()

insights_list = [
    f"1. Highest Revenue Category: '{top_cat_row['Category']}' dominates sales with INR {top_cat_row['Total_Revenue']:,.2f}.",
    f"2. Top Selling Product: '{top_prod_row['Product']}' recorded the highest sales volume ({top_prod_row['Total_Quantity_Sold']} units).",
    f"3. Top Revenue City: '{top_city_row['City']}' generated the highest revenue (INR {top_city_row['Total_Revenue']:,.2f}).",
    f"4. Least Performing Category: '{lowest_cat_row['Category']}' generated lowest revenue (INR {lowest_cat_row['Total_Final_Revenue']:,.2f} after discount).",
    "5. Inventory Recommendation: Maintain higher stock levels for Electronics and top-volume items like TShirts and Mobiles.",
    "6. Marketing Recommendation: Focus promotional campaigns and bundling on lower-performing categories like Clothing."
]

for insight in insights_list:
    print(insight)

save_output("12_business_insights.txt", "===== BUSINESS INSIGHTS =====\n" + "\n".join(insights_list) + "\n")

print("\n[SUCCESS] PySpark execution completed successfully! All output files generated in 'outputs/' directory.")

spark.stop()
