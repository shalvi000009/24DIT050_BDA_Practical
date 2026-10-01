# Practical 9: Social Media Trend Detection Using PySpark

## 📌 Practical Overview
* **Title:** Social Media Trend Detection Using PySpark
* **Course:** Big Data Analytics (BDA)
* **Domain:** Social Media Analytics & Mass Data Processing
* **Technology Stack:** Apache Spark 3.5.1, PySpark, Python 3.11, Matplotlib, Seaborn, Pandas
* **Execution Environment:** VS Code / Jupyter Notebook (Local Standalone Spark Session)

---

## 🏢 Scenario & Problem Statement
You are a Big Data Analyst at **TrendPulse Social Media Analytics**. The company monitors continuous streams of social media feeds from platforms such as **Twitter/X, Instagram, Facebook, and LinkedIn**. Millions of public posts contain hashtags associated with current events, technological breakthroughs, sports tournaments, entertainment releases, and stock market shifts.

### Problem
Traditional database engines and single-threaded Python analytics tools fail to process multi-gigabyte hashtag streams efficiently. Without distributed in-memory compute frameworks, trend detection suffers from latency, memory exhaustion, and sluggish aggregation performance.

### Solution
This project implements a high-performance **PySpark DataFrame pipeline** that validates, cleans, aggregates, ranks, categorizes, and visualizes hashtag activity feeds at scale using Apache Spark's distributed data structures and windowing functions.

---

## 🎯 Key Objectives
1. **Spark Session Initialization:** Configure a local PySpark environment without requiring a dedicated Hadoop cluster.
2. **Data Ingestion & Inspection:** Load social media hashtag datasets and validate schemas.
3. **Data Quality & Normalization:** Filter empty/null records, strip leading/trailing whitespace, and normalize casing (`#ai`, `#Ai` -> `#AI`).
4. **Core Trend Analytics:** Compute global hashtag frequency counts (`groupBy().count().orderBy()`).
5. **Top Trend Identification:** Dynamically extract top trending topics, specifically highlighting the **Top 3 Trending Hashtags**.
6. **Multi-Day Window Analytics:** Track daily momentum and date-by-date top hashtags using `Window.partitionBy("Date")`.
7. **Multi-Dimensional Segmentation:** Analyze hashtag distributions across geographic **Regions** and **Platforms**.
8. **Rule-Based Categorization:** Classify hashtags into Technology, Sports, Entertainment, Business, and Other using PySpark `when()` expressions.
9. **Visual Analytics:** Generate publication-ready visualizations of trends, daily timelines, and domain distributions.
10. **Executive Reporting:** Generate an automated text report with data-driven business recommendations.

---

## 📂 Project Structure
```
Practical-9/
│
├── data/
│   └── hashtags.csv               # Input raw dataset (Hashtag, Date, Region, Platform)
│
├── output/
│   ├── hashtag_frequency.csv      # Complete frequency count of all cleaned hashtags
│   ├── top_trends.csv             # Top 10 trending hashtags
│   ├── trend_report.txt           # Comprehensive 17-section executive analysis report
│   └── charts/                    # Generated analytical charts
│       ├── top_hashtags.png       # Bar chart of Top 10 hashtags
│       ├── daily_trends.png       # Multi-line timeline of top 5 hashtags
│       ├── category_distribution.png # Donut chart of category breakdown
│       └── regional_hashtags.png  # Grouped bar chart comparing regions
│
├── practical9.py                  # Main modular PySpark pipeline script
├── requirements.txt               # Python package dependencies
└── README.md                      # Complete project documentation & viva guide
```

---

## 📊 Dataset Schema & Fields
The dataset `data/hashtags.csv` contains simulated social media activity feeds generated with a fixed random seed (`seed=42`) for 100% reproducibility.

| Column Name | Data Type | Description | Example Values |
| :--- | :--- | :--- | :--- |
| `Hashtag` | String | Hashtag text from social post feed | `#AI`, `#Cricket`, `#Technology`, ` #ai ` |
| `Date` | Date | Post timestamp (YYYY-MM-DD) | `2026-09-01`, `2026-09-02` |
| `Region` | String | Originating geographic region | `Gujarat`, `Maharashtra`, `Delhi`, `Karnataka` |
| `Platform` | String | Social network platform | `Twitter`, `Instagram`, `Facebook`, `LinkedIn` |

---

## ⚙️ Installation & Execution Instructions

### Prerequisites
* Python 3.11+
* Java Runtime Environment (JRE/JDK 8 or 11+) for PySpark execution

### Step 1: Clone / Navigate to Directory
```bash
cd Practical-9
```

### Step 2: Set Up Virtual Environment (Recommended)
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3: Install Required Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Run the PySpark Pipeline
```bash
python practical9.py
```

---

## ⚡ PySpark Operations & Key Functions Used

1. **`SparkSession.builder.appName("Practical9_SocialMediaTrendDetection").master("local[*]").getOrCreate()`**
   * Configures local Spark driver using all available CPU threads.

2. **`spark.read.csv("data/hashtags.csv", header=True, inferSchema=True)`**
   * Reads raw CSV file into a Spark DataFrame with automated schema inference.

3. **`filter(col("Hashtag").isNotNull() & (trim(col("Hashtag")) != ""))`**
   * Cleans missing or blank rows at scale.

4. **`trim()` and `upper()` Functions**
   * Removes leading/trailing spaces and converts text to uppercase, unifying case variations like `#ai`, `#Ai`, `#AI` into `#AI`.

5. **`groupBy("Hashtag").agg(count("Hashtag").alias("Frequency")).orderBy(desc("Frequency"))`**
   * Core aggregation pipeline that counts mentions per tag and sorts descending.

6. **Spark Window Functions (`Window.partitionBy("Date").orderBy(desc("count"))`)**
   * Computes per-group rankings without shuffling entire data across partitions.
   * `row_number().over(window)` and `rank().over(window)` assign partition-specific ranks to identify daily and regional leaders.

7. **Conditional Mapping (`when().when().otherwise()`)**
   * Categorizes hashtags into domain buckets (Technology, Sports, Entertainment, Business, Other) directly on the Spark cluster.

8. **`toPandas()` for Small Aggregated Results**
   * Collects *only final summary tables* to driver memory for Matplotlib/Seaborn visualization, preserving Spark performance.

---

## 📈 Summary of Execution & Expected Output

Upon running `practical9.py`, the output terminal displays step-by-step progress:

```
================================================================================
      PYSPARK SOCIAL MEDIA TREND DETECTION - BDA PRACTICAL 9                    
================================================================================
-> PySpark Session created successfully. Spark Version: 3.5.1
[STEP 2] Loading Social Media Hashtag Dataset...
-> Total Raw Records Loaded: 1500

[STEP 3 & 4] Data Validation and Hashtag Cleaning...
-> Cleaned Records Count: 1490
-> Distinct Hashtag Count: 28

[STEP 5, 6, 7] Core Trend Detection...
-> Exported full hashtag frequency table to 'output/hashtag_frequency.csv'.

==================================================
TOP 3 TRENDING HASHTAGS (DYNAMIC SPARK RESULT)
==================================================
  1. #CRICKET             - Frequency: 128 occurrences
  2. #AI                  - Frequency: 115 occurrences
  3. #TECHNOLOGY          - Frequency: 103 occurrences
==================================================

[SUPPLEMENTARY] Multi-Day Trend Analysis using Spark Window Functions...
+----------+--------+-----------+----+
|Date      |Hashtag |Daily_Count|Rank|
+----------+--------+-----------+----+
|2026-09-01|#CRICKET|30         |1   |
|2026-09-02|#CRICKET|21         |1   |
|2026-09-03|#AI     |28         |1   |
|2026-09-04|#STARTUP|27         |1   |
|2026-09-05|#CRICKET|29         |1   |
+----------+--------+-----------+----+

[STEP 9 & SUPPLEMENTARY] Generating Visualizations in 'output/charts'...
-> Saved Chart 1: 'output/charts/top_hashtags.png'
-> Saved Chart 2: 'output/charts/daily_trends.png'
-> Saved Chart 3: 'output/charts/category_distribution.png'
-> Saved Chart 4: 'output/charts/regional_hashtags.png'

[STEP 10 & REPORT] Writing Executive Trend Analysis Report to 'output/trend_report.txt'...
-> Successfully written executive report to 'output/trend_report.txt'.
```

---

## 💡 Business Interpretation & Marketing Recommendations
1. **High Volume Topics (#CRICKET, #AI, #TECHNOLOGY):** Represent the highest engagement drivers across social platforms. Marketing teams should prioritize ad space around these topics.
2. **Platform Customization:** Professional feeds (LinkedIn) favor #STARTUP and #AI, whereas entertainment platforms (Instagram/Twitter) favor #MOVIES and #CRICKET.
3. **Regional Target Marketing:** Geographic windowing shows strong cricket interest in Gujarat & Maharashtra, while Delhi exhibits higher concentration in startup and business discussions.

---

## 🎓 Viva Voce Questions & Answers (25 Comprehensive Q&As)

### 1. What is PySpark?
**Answer:** PySpark is the Python API for Apache Spark. It enables Python developers to perform distributed data processing, real-time streaming, SQL queries, and machine learning over large datasets using Spark's core engine.

### 2. Why use Apache Spark instead of Pandas for social media analytics?
**Answer:** Pandas processes data in-memory on a single CPU core and fails when datasets exceed driver memory. PySpark distributes data across cluster nodes and multiple cores, executing computations in parallel with lazy evaluation and resilient fault tolerance.

### 3. What is a Spark DataFrame?
**Answer:** A Spark DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a Pandas DataFrame, but optimized under the hood by Spark's Catalyst Optimizer.

### 4. What does `groupBy()` do in PySpark?
**Answer:** `groupBy()` collects records that share identical values in specified columns (e.g., `Hashtag`) so aggregate operations like `count()`, `sum()`, or `avg()` can be computed per group across partitions.

### 5. What does `count()` do?
**Answer:** `count()` is an aggregation function (or action) that computes the total number of rows in a DataFrame or within each grouped category.

### 6. How do we sort hashtags by frequency in PySpark?
**Answer:** We chain `.orderBy(desc("Frequency"))` or `.sort(col("Frequency").desc())` onto the aggregated DataFrame.

### 7. What is the difference between `orderBy()` and `sort()` in PySpark?
**Answer:** In PySpark DataFrames, `orderBy()` is an alias for `sort()`. Both return a DataFrame sorted by specified expressions.

### 8. What is the difference between a Transformation and an Action in Spark?
**Answer:** 
* **Transformation:** Returns a new DataFrame/RDD without executing immediate computation (e.g., `filter()`, `select()`, `groupBy()`). It is lazily evaluated.
* **Action:** Triggers physical computation on the cluster and returns a result to the driver or writes to disk (e.g., `count()`, `collect()`, `show()`, `write.csv()`).

### 9. What is Lazy Evaluation in Apache Spark?
**Answer:** Lazy evaluation means Spark does not compute execution steps immediately when transformations are declared. Instead, it builds a Logical DAG (Directed Acyclic Graph) of operations and optimizes the entire execution plan before an action triggers execution.

### 10. Why is hashtag normalization required in social media datasets?
**Answer:** Users post hashtags with varied capitalization (`#ai`, `#Ai`, `#AI`) and extra spaces (` #AI `). Without normalization (trimming whitespace and standardizing case), Spark would count them as separate hashtags, skewing frequency analysis.

### 11. Why strip leading and trailing whitespace?
**Answer:** Spaces injected by user input or serialization (` #AI ` vs `#AI`) prevent exact equality matching during `groupBy()` operations.

### 12. How are the Top 3 trending hashtags dynamically identified?
**Answer:** By taking the top 3 rows of the sorted frequency DataFrame using `.limit(3).collect()` or `.take(3)`, extracting actual result values directly from Spark.

### 13. How does Spark handle large datasets on a single machine?
**Answer:** Spark splits large files into internal partitions and processes them concurrently across available local CPU cores (`local[*]`) using multi-threading.

### 14. What is a Spark Partition?
**Answer:** A partition is a logical chunk of a distributed dataset. Spark processes each partition in parallel on executor cores.

### 15. What is a Window Function in PySpark?
**Answer:** A Window function performs calculations across a set of rows (a "window") that are related to the current row, without collapsing rows into a single aggregate row like `groupBy()` does.

### 16. Why use `rank()` or `row_number()` with Window functions?
**Answer:** They allow ranking rows within specific subgroups (e.g., ranking top hashtags per `Date` or `Region`) to easily filter top N items per category.

### 17. How are trends compared across multiple days in this project?
**Answer:** Data is grouped by `Date` and `Hashtag`, then ranked per day using `Window.partitionBy("Date").orderBy(desc("count"))`.

### 18. How are regional hashtag variations analyzed?
**Answer:** By grouping by `Region` and `Hashtag`, then applying `Window.partitionBy("Region").orderBy(desc("count"))` to find popular tags per region.

### 19. Why should large PySpark datasets NOT be collected to the driver using `.collect()`?
**Answer:** `.collect()` pulls all distributed data into driver node RAM. Executing `.collect()` on millions of rows causes `OutOfMemoryError` crashes on the driver.

### 20. What is the difference between PySpark and Pandas DataFrames?
**Answer:**
* **Pandas:** In-memory, single-node, eager evaluation, mutable.
* **PySpark:** Distributed memory, multi-node cluster, lazy evaluation, immutable.

### 21. What is Data Skew in Big Data analytics?
**Answer:** Data skew occurs when a disproportionate amount of data falls into a single partition (e.g., one viral hashtag like `#AI` having millions of rows), causing one worker core to bottleneck the entire job.

### 22. Why can some hashtags exhibit extremely high frequency?
**Answer:** Viral events, news, sporting events, or automated bot activities create huge spikes in specific hashtag occurrences.

### 23. Give three examples of Spark Transformations used in Practical 9.
**Answer:** `filter()`, `withColumn()`, `groupBy()`.

### 24. Give three examples of Spark Actions used in Practical 9.
**Answer:** `count()`, `show()`, `take()`.

### 25. How can real-time social media trend detection be implemented in PySpark?
**Answer:** By using **Spark Structured Streaming**, connecting Spark to a streaming source like Apache Kafka or WebSocket feeds, and performing windowed aggregations over sliding time intervals.

---

## 🏆 Summary Checklist for Practical Evaluation
- [x] SparkSession created with local execution mode
- [x] Social media CSV dataset loaded into Spark DataFrame
- [x] Data validation & casing/whitespace normalization complete
- [x] GroupBy aggregation & frequency calculation performed
- [x] Top 3 trending hashtags extracted dynamically
- [x] Window functions used for multi-day date analysis
- [x] Regional & platform distributions computed
- [x] Rule-based hashtag categorization implemented
- [x] 4 analytical charts saved in `output/charts/`
- [x] Executive trend analysis report saved in `output/trend_report.txt`
- [x] 25 viva questions documented in `README.md`
