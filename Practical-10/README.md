# Practical 10: Scalable K-Means Clustering on Customer Segmentation Data using Apache Spark MLlib

## 📌 Title & Overview
**Scalable K-Means Clustering on Customer Segmentation Data using Apache Spark MLlib (Parallel K-Means)**

**Student ID:** 24DIT050  
- **Academic Context:** Big Data Analytics (BDA) University Practical Laboratory
- **Author/Student:** B.Tech Computer Engineering / Information Technology
- **Execution Environment:** Apache Spark 3.x, PySpark, Hadoop HDFS, Python 3.11

---

## 🏬 Real-World Industry Scenario
You are working as a Senior Data Scientist at **RetailEdge Analytics**. The marketing team needs to segment over **10 million customer profiles** based on historical transaction behavior to launch highly targeted, personalized marketing campaigns (e.g., loyalty rewards for high-spenders, re-engagement offers for at-risk churn customers, and welcome discounts for recent buyers).

### Why Traditional ML Fails at Scale:
Traditional single-node clustering tools (such as standard `scikit-learn` in Python or `R`) load the entire dataset into driver RAM. When handling millions of multi-feature customer records:
1. **Memory Limitations:** Datasets exceed single-node RAM capacities, causing Out-Of-Memory (`OOM`) heap crashes.
2. **Computational Bottlenecks:** Calculating pairwise Euclidean distances for millions of records sequentially requires exorbitant processing time.

### Solution with Apache Spark MLlib:
Apache Spark MLlib solves this by executing **Parallel K-Means**. Dataset partitions are distributed across worker nodes in a cluster, and distance computations and centroid updates are computed concurrently across Spark executors using parallel map-reduce operations.

---

## 🎯 Objectives & Required Tasks
1. **Data Loading & Preprocessing:** Ingest large-scale transaction CSV data from local storage or HDFS into a PySpark DataFrame (`header=True`, `inferSchema=True`). Validate schema, partition count, and data quality (nulls, duplicates, negative values).
2. **Feature Extraction:** Use `VectorAssembler` to group key behavioral features:
   - `total_spend`
   - `purchase_count`
   - `avg_basket_size`
   - `days_since_last_purchase`
   *(Note: `customer_id` is explicitly excluded from the clustering vector).*
3. **Feature Scaling:** Apply `StandardScaler` (`withMean=True`, `withStd=True`) to normalize feature distributions so large numerical scales (e.g., `total_spend`) do not overwhelm smaller ones (e.g., `purchase_count`).
4. **Spark Parallel K-Means:** Train Spark MLlib `KMeans` with $k = 5$, `maxIter = 20`, and `seed = 42`.
5. **Cluster Evaluation (WSSSE):** Evaluate cluster compactness using Within Set Sum of Squared Errors (WSSSE).
6. **Hyperparameter Tuning (Elbow Method):** Train models for $k \in \{3, 4, 5, 6, 7\}$, plot `elbow_curve.png`, and evaluate optimal cluster separation.
7. **Cluster Summary & Interpretation:** Map cluster assignments to original records, compute cluster mean statistics, and dynamically assign human-readable business personas.
8. **Visualizations:** Generate PCA 2D cluster scatter plots and standardized cluster center heatmaps/bar charts.
9. **Supplementary Experiments:**
   - **Bisecting K-Means:** Train hierarchical divisive `BisectingKMeans` and compare with standard Parallel K-Means.
   - **Single-Node vs Distributed Benchmark:** Compare single-thread `scikit-learn` vs PySpark Parallel K-Means on controlled samples.
   - **Spark Partitioning:** Analyze the effect of partition counts (2, 4, 8) on parallelism and execution speed.
10. **Report & Deliverables Generation:** Produce all formatted output CSVs, PNGs, and a text report `practical10_report.txt`.

---

## 🛠️ Technologies & Tools Used
- **Engine:** Apache Spark 3.x (PySpark)
- **ML Framework:** Spark MLlib (`pyspark.ml.clustering`, `pyspark.ml.feature`)
- **Storage/Cluster:** Hadoop HDFS / Local File System
- **Language:** Python 3.11
- **Data Analytics:** Pandas, NumPy, Scikit-Learn
- **Visualization:** Matplotlib, Seaborn

---

## 📁 Project Structure
```text
Practical-10/
│
├── data/
│   └── retail_customers.csv          # Generated simulated customer dataset (20,000 records)
│
├── output/
│   ├── cluster_assignments.csv       # Sample customer records with cluster prediction IDs
│   ├── cluster_summary.csv           # Per-cluster customer counts and feature averages
│   ├── wssse_results.csv             # WSSSE & training time metrics across k=3..7
│   ├── performance_comparison.csv    # Scikit-Learn vs Spark Parallel K-Means metrics
│   ├── elbow_curve.png               # Elbow method visualization plot
│   ├── cluster_visualization.png     # 2D PCA cluster scatter plot
│   ├── cluster_centers.png           # Heatmap of standardized cluster centroid values
│   ├── bisecting_kmeans_comparison.csv # Bisecting K-Means vs Standard K-Means metrics
│   └── practical10_report.txt        # Comprehensive execution summary report
│
├── practical10.py                    # Main PySpark clustering pipeline script
├── generate_dataset.py               # Dataset generation script with 5 persona distributions
├── requirements.txt                  # Python dependencies
└── README.md                         # Detailed project guide & viva preparation
```

---

## 📊 Dataset Schema & Description
The simulated dataset contains **20,000 records** for fast, reliable local practical demonstration (while preserving scalability for the full 10M record HDFS university environment).

| Field Name | Data Type | Description | Included in ML Feature Vector? |
| :--- | :--- | :--- | :---: |
| `customer_id` | String | Unique customer surrogate key (e.g. `CUST_10001`) | ❌ **No** (Excluded to prevent ID bias) |
| `total_spend` | Double | Cumulative dollar spend ($) | ✅ **Yes** |
| `purchase_count` | Integer | Total completed transaction count | ✅ **Yes** |
| `avg_basket_size` | Double | Average dollar value per order ($) | ✅ **Yes** |
| `days_since_last_purchase` | Integer | Days elapsed since last purchase (Recency) | ✅ **Yes** |
| `product_category_preference` | Integer | Preferred product category code (1–5) | ⚙️ Optional |

---

## 🚀 Setup & Execution Guide

### 1. Prerequisites & Environment Setup
Open a terminal (PowerShell, Command Prompt, or Bash) in the project directory:

```bash
# Create a virtual environment (optional but recommended)
python -m venv .venv

# Activate virtual environment
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Windows Command Prompt:
.venv\Scripts\activate.bat
# Linux / macOS:
source .venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### 2. Generate Simulated Customer Dataset
Run the data generator to create `data/retail_customers.csv`:

```bash
python generate_dataset.py
```

### 3. Run Practical 10 Pipeline (Local Mode)
Execute the complete Spark MLlib pipeline:

```bash
python practical10.py
```

---

## 🌐 HDFS Deployment & Big Data Execution
In a university Hadoop cluster environment where the dataset is stored in HDFS at `/user/student/retail_customers.csv`:

### 1. Upload Local Data to HDFS
```bash
# Create student directory in HDFS
hdfs dfs -mkdir -p /user/student

# Upload local CSV to HDFS
hdfs dfs -put data/retail_customers.csv /user/student/retail_customers.csv

# Verify HDFS directory contents
hdfs dfs -ls /user/student/
```

### 2. Run Practical 10 pointing to HDFS Path
```bash
python practical10.py --input hdfs:///user/student/retail_customers.csv
```

---

## 📈 Detailed Output Explanations

1. **`wssse_results.csv`**: Contains WSSSE error values and training runtimes for $k \in \{3, 4, 5, 6, 7\}$.
2. **`elbow_curve.png`**: Graphical plot showing WSSSE vs $k$. The "elbow point" indicates the point of diminishing marginal error reduction (chosen $k=5$).
3. **`cluster_summary.csv`**: Aggregated table showing customer volume, average spend, average purchase frequency, average basket size, and average recency for each of the 5 clusters.
4. **`cluster_assignments.csv`**: First 1,000 customer rows with their original features and assigned cluster integer (0 to 4).
5. **`cluster_visualization.png`**: 2D scatter plot using Principal Component Analysis (PCA) to project 4D scaled feature space into 2 principal components.
6. **`cluster_centers.png`**: Heatmap illustrating the normalized z-score values of each centroid across features.
7. **`bisecting_kmeans_comparison.csv`**: Comparison metrics between divisive Bisecting K-Means and standard Parallel K-Means.
8. **`performance_comparison.csv`**: Benchmark metrics between single-node Scikit-Learn KMeans and distributed Spark MLlib KMeans.
9. **`practical10_report.txt`**: Complete text report containing empirical measurements, business findings, and conclusion.

---

## 🧠 Core Machine Learning & Big Data Concepts

### 1. K-Means Clustering & Euclidean Distance
K-Means is an unsupervised iterative algorithm that partitions $N$ observations into $K$ clusters. Each observation is assigned to the cluster with the nearest mean centroid:
$$d(x, c) = \sqrt{\sum_{i=1}^{n} (x_i - c_i)^2}$$

### 2. Why Feature Scaling is Essential
Without scaling, features with large raw values (e.g., `total_spend` up to $20,000) completely dominate distance computations, rendering smaller features (e.g., `purchase_count` from 1 to 50) irrelevant. `StandardScaler` standardizes each feature to:
$$z = \frac{x - \mu}{\sigma}$$
where $\mu$ is mean and $\sigma$ is standard deviation.

### 3. Within Set Sum of Squared Errors (WSSSE)
WSSSE measures the compactness of clusters:
$$\text{WSSSE} = \sum_{i=1}^{K} \sum_{x \in C_i} ||x - c_i||^2$$
Lower WSSSE indicates tighter clusters, but WSSSE naturally decreases as $k$ increases, necessitating the Elbow Method.

### 4. Spark Parallel K-Means Architecture
Spark MLlib uses **Parallel K-Means (K-Means||)**:
- **Initialization:** Parallelized sampling to choose initial centroids spread across cluster space.
- **Iteration:** Map operations on RDD/DataFrame partitions calculate local point-to-centroid distance accumulators concurrently across worker executor threads.
- **Reduce:** Centroids are updated via global reduction drivers.

---

## ❓ 25+ Viva Voce Questions & Answers

### Q1: What is the primary title and objective of Practical 10?
**Ans:** Scalable K-Means Clustering on Customer Segmentation Data using Apache Spark MLlib. The objective is to partition large-scale customer transaction data into 5 distinct behavioral segments using distributed parallel computing.

### Q2: Why is Apache Spark MLlib preferred over Scikit-Learn for 10M customer records?
**Ans:** Scikit-Learn executes on a single node and relies on single-instance RAM. Processing 10M records in Scikit-Learn leads to Out-Of-Memory (OOM) crashes. Spark MLlib distributes data across worker nodes and computes distances in parallel.

### Q3: Why is `customer_id` excluded from the VectorAssembler?
**Ans:** `customer_id` is a surrogate key identifier. Treating it as a numerical feature introduces artificial distance bias that ruins customer distance metrics.

### Q4: What role does `VectorAssembler` play in PySpark ML pipelines?
**Ans:** `VectorAssembler` is a PySpark transformer that combines multiple individual numeric feature columns into a single unified dense/sparse feature vector column required by Spark ML estimators.

### Q5: Why is `StandardScaler` necessary before running K-Means?
**Ans:** K-Means calculates Euclidean distance. Features with large scales (e.g. `total_spend` in thousands) would dominate features with small scales (e.g. `purchase_count` 1–50) if not standardized to mean 0 and variance 1.

### Q6: What parameters were set for `StandardScaler` in this practical?
**Ans:** `withMean=True` (subtracts mean to center features at zero) and `withStd=True` (scales features to unit standard deviation).

### Q7: What is WSSSE and how is it calculated?
**Ans:** WSSSE stands for Within Set Sum of Squared Errors. It measures the sum of squared Euclidean distances from each data point to its assigned cluster centroid.

### Q8: What is the Elbow Method and why do we use it?
**Ans:** The Elbow Method plots WSSSE against different values of $k$ (e.g., $k=3..7$). The optimal $k$ is identified at the "elbow point", where adding more clusters yields diminishing reductions in WSSSE.

### Q9: Why shouldn't $k$ be chosen solely by minimizing WSSSE?
**Ans:** WSSSE continuously decreases as $k$ increases and reaches 0 when $k = N$ (where every point is its own cluster). Choosing $k$ strictly by minimum WSSSE would lead to severe overfitting.

### Q10: What value of $k$ was selected for the primary implementation?
**Ans:** $k = 5$, representing 5 main customer personas: High-Value Loyal, Frequent Moderate Spenders, Low-Value Occasional, At-Risk Inactive, and New/Recent Bargain Buyers.

### Q11: What is Bisecting K-Means?
**Ans:** Bisecting K-Means is a hierarchical divisive clustering algorithm. It starts with all points in one cluster and iteratively splits clusters using standard 2-means clustering until $k$ clusters are formed.

### Q12: How does Bisecting K-Means differ from standard K-Means?
**Ans:** Standard K-Means initializes $k$ centroids simultaneously and updates them globally. Bisecting K-Means builds a cluster tree top-down, often producing more consistent hierarchical cluster structures.

### Q13: Why is PCA used in `visualize_clusters()`?
**Ans:** Principal Component Analysis (PCA) reduces 4-dimensional scaled feature vectors into 2 principal components (`PC1`, `PC2`) strictly for 2D visual plotting on a 2D scatter graph.

### Q14: Does PCA replace the main K-Means feature pipeline in this practical?
**Ans:** No. K-Means clustering is trained directly on the 4D scaled feature vectors. PCA is applied solely as a downstream visualization step on a sampled subset.

### Q15: What is the role of `header=True` and `inferSchema=True` when loading CSV data in PySpark?
**Ans:** `header=True` instructs Spark to treat the first row of CSV as column names. `inferSchema=True` causes Spark to scan data to automatically assign appropriate data types (Integer, Double, String).

### Q16: How does Spark handle dataset partitioning?
**Ans:** Spark splits DataFrames into chunks called partitions. Each partition is processed independently by an executor thread in parallel.

### Q17: What happens if a dataset has too few or too many partitions?
**Ans:** Too few partitions cause under-utilization of multi-core executor resources. Too many partitions introduce excessive task scheduling overhead and network shuffle traffic.

### Q18: What is the difference between Spark Transformations and Actions?
**Ans:** Transformations (e.g., `filter()`, `select()`, `withColumn()`, `VectorAssembler.transform()`) are lazily evaluated and construct a DAG. Actions (e.g., `count()`, `show()`, `collect()`, `fit()`) trigger physical execution.

### Q19: Why should `collect()` be avoided on 10 million record DataFrames?
**Ans:** `collect()` pulls all distributed data records from worker nodes into the single driver node's memory, causing instant Out-Of-Memory driver heap crashes.

### Q20: How did we safely visualize data without calling `collect()` on the entire dataset?
**Ans:** We used `.sample(fraction=...)` and `.limit()` to extract a small, representative sample (e.g., 5,000 rows) into Pandas for Matplotlib/Seaborn rendering.

### Q21: What is `seed=42` used for in K-Means?
**Ans:** `seed=42` sets a fixed pseudo-random number generator seed for centroid initialization, guaranteeing reproducible clustering results across runs.

### Q22: How is a PySpark DataFrame stored in HDFS accessed in code?
**Ans:** By passing the HDFS path URI: `spark.read.csv("hdfs://localhost:9000/user/student/retail_customers.csv", header=True, inferSchema=True)`.

### Q23: How do you check duplicate customer IDs in PySpark?
**Ans:** `total_rows - df.select("customer_id").distinct().count()`.

### Q24: What is the difference between `maxIter` and convergence in Spark KMeans?
**Ans:** `maxIter` sets the maximum allowable iterations. The algorithm terminates early if centroid positions change by less than a tiny tolerance value before `maxIter` is reached.

### Q25: How do single-node Scikit-Learn and Spark MLlib compare on small vs large datasets?
**Ans:** On small local datasets (<100K rows), Scikit-Learn is faster due to zero JVM/Spark context startup overhead. On massive datasets (>10M rows), Spark MLlib scales horizontally across clusters where Scikit-Learn fails.

---

## 🏆 Conclusion & Summary
Practical 10 successfully demonstrates the end-to-end architecture of **Scalable Parallel K-Means Customer Segmentation using Apache Spark MLlib**. All university lab criteria, dataset preprocessing rules, scaling requirements, hyperparameter evaluations, and supplementary benchmarks have been fully satisfied and validated.
