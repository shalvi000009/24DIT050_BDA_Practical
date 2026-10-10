"""
generate_dataset.py
-------------------
Practical 10: Scalable K-Means Clustering on Customer Segmentation Data using Apache Spark MLlib
Scenario: RetailEdge Analytics Customer Transaction Dataset Generation

This script generates a reproducible, multi-segment customer transaction dataset
simulating customer purchase behavior across 5 distinct persona profiles.

Dataset Schema:
  - customer_id: Unique Identifier for each customer (e.g., CUST_10001)
  - total_spend: Total cumulative expenditure ($)
  - purchase_count: Total number of completed transactions
  - avg_basket_size: Average dollar amount spent per transaction ($)
  - days_since_last_purchase: Recency of last purchase (in days)
  - product_category_preference: Numerical code (1-5) representing top product category

Note on Scale:
  For local practical demonstration, this generates a scaled-down 20,000 record dataset.
  The production university scenario uses ~10 million records stored in HDFS.
"""

import os
import numpy as np
import pandas as pd

def generate_customer_data(num_records=20000, seed=42):
    """
    Generates synthetic customer segmentation dataset with 5 realistic clusters.
    
    Parameters:
        num_records (int): Total number of customer records to generate.
        seed (int): Random seed for reproducibility.
        
    Returns:
        pd.DataFrame: DataFrame containing generated customer transaction data.
    """
    np.random.seed(seed)
    
    # Define cluster distribution proportions (sum to 1.0)
    proportions = [0.20, 0.25, 0.25, 0.15, 0.15]
    counts = [int(p * num_records) for p in proportions]
    counts[-1] = num_records - sum(counts[:-1])  # Ensure total exactly equals num_records
    
    data_segments = []
    
    # Segment 1: High-Value Loyal Customers (High spend, high purchases, high basket, low recency)
    s1_spend = np.random.normal(8500, 1500, counts[0]).clip(3500, 20000)
    s1_count = np.random.normal(70, 12, counts[0]).clip(35, 150).astype(int)
    s1_basket = np.random.normal(125, 20, counts[0]).clip(50, 250)
    s1_recency = np.random.normal(12, 4, counts[0]).clip(1, 40).astype(int)
    s1_cat = np.random.choice([1, 2], size=counts[0], p=[0.7, 0.3])
    df1 = pd.DataFrame({
        'total_spend': np.round(s1_spend, 2),
        'purchase_count': s1_count,
        'avg_basket_size': np.round(s1_basket, 2),
        'days_since_last_purchase': s1_recency,
        'product_category_preference': s1_cat
    })
    data_segments.append(df1)
    
    # Segment 2: Frequent Moderate Spenders (Moderate spend, high frequency, moderate basket, low recency)
    s2_spend = np.random.normal(3600, 700, counts[1]).clip(1500, 6000)
    s2_count = np.random.normal(48, 10, counts[1]).clip(20, 90).astype(int)
    s2_basket = np.random.normal(75, 12, counts[1]).clip(30, 140)
    s2_recency = np.random.normal(22, 6, counts[1]).clip(2, 55).astype(int)
    s2_cat = np.random.choice([2, 3], size=counts[1], p=[0.6, 0.4])
    df2 = pd.DataFrame({
        'total_spend': np.round(s2_spend, 2),
        'purchase_count': s2_count,
        'avg_basket_size': np.round(s2_basket, 2),
        'days_since_last_purchase': s2_recency,
        'product_category_preference': s2_cat
    })
    data_segments.append(df2)
    
    # Segment 3: Low-Value Occasional Customers (Low spend, low frequency, low basket, high recency)
    s3_spend = np.random.normal(450, 150, counts[2]).clip(50, 1000)
    s3_count = np.random.normal(6, 2, counts[2]).clip(1, 15).astype(int)
    s3_basket = np.random.normal(65, 15, counts[2]).clip(20, 120)
    s3_recency = np.random.normal(130, 35, counts[2]).clip(30, 300).astype(int)
    s3_cat = np.random.choice([3, 4], size=counts[2], p=[0.5, 0.5])
    df3 = pd.DataFrame({
        'total_spend': np.round(s3_spend, 2),
        'purchase_count': s3_count,
        'avg_basket_size': np.round(s3_basket, 2),
        'days_since_last_purchase': s3_recency,
        'product_category_preference': s3_cat
    })
    data_segments.append(df3)
    
    # Segment 4: At-Risk Inactive Customers (Moderate/high historical spend, low frequency, high recency)
    s4_spend = np.random.normal(2400, 600, counts[3]).clip(800, 5000)
    s4_count = np.random.normal(22, 5, counts[3]).clip(5, 45).astype(int)
    s4_basket = np.random.normal(105, 20, counts[3]).clip(40, 180)
    s4_recency = np.random.normal(240, 40, counts[3]).clip(120, 365).astype(int)
    s4_cat = np.random.choice([1, 4, 5], size=counts[3], p=[0.4, 0.4, 0.2])
    df4 = pd.DataFrame({
        'total_spend': np.round(s4_spend, 2),
        'purchase_count': s4_count,
        'avg_basket_size': np.round(s4_basket, 2),
        'days_since_last_purchase': s4_recency,
        'product_category_preference': s4_cat
    })
    data_segments.append(df4)
    
    # Segment 5: New / Recent Bargain Buyers (Low/Moderate spend, low count, low recency)
    s5_spend = np.random.normal(850, 250, counts[4]).clip(100, 2000)
    s5_count = np.random.normal(12, 3, counts[4]).clip(2, 25).astype(int)
    s5_basket = np.random.normal(68, 12, counts[4]).clip(25, 110)
    s5_recency = np.random.normal(8, 3, counts[4]).clip(1, 20).astype(int)
    s5_cat = np.random.choice([3, 5], size=counts[4], p=[0.5, 0.5])
    df5 = pd.DataFrame({
        'total_spend': np.round(s5_spend, 2),
        'purchase_count': s5_count,
        'avg_basket_size': np.round(s5_basket, 2),
        'days_since_last_purchase': s5_recency,
        'product_category_preference': s5_cat
    })
    data_segments.append(df5)
    
    # Combine and shuffle
    df_combined = pd.concat(data_segments, ignore_index=True)
    df_combined = df_combined.sample(frac=1, random_state=seed).reset_index(drop=True)
    
    # Assign unique customer IDs
    df_combined.insert(0, 'customer_id', [f'CUST_{10001 + i}' for i in range(len(df_combined))])
    
    return df_combined

def main():
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    file_path = os.path.join(data_dir, "retail_customers.csv")
    
    print("Generating simulated customer transaction dataset (20,000 records)...")
    df = generate_customer_data(num_records=20000, seed=42)
    
    df.to_csv(file_path, index=False)
    print(f"Dataset successfully created and saved to '{file_path}'.")
    print(f"Total Rows: {len(df)}")
    print("Sample Data:")
    print(df.head(5))

if __name__ == "__main__":
    main()
