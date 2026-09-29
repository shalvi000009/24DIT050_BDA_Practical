# Big Data Analytics Practical 7: Performance Optimization Using Caching and Partitioning

## 📌 Practical Overview

* **Practical Title:** Performance Optimization Using Caching and Partitioning
* **Student ID:** 24DIT050
* **Scenario:** MovieFlix Technologies operates a large-scale movie recommendation system that repeatedly processes millions of user movie ratings. As the volume of data grows, query execution time becomes a critical bottleneck.
* **Goal:** Optimize an Apache Spark application using DataFrame `cache()`, `persist()`, `repartition()`, and `coalesce()` to reduce execution time, prevent redundant DAG computation, and improve scalability.
* **Technology Stack:** Python 3.11, PySpark 3.5.x, Apache Spark 3.x, Matplotlib / Pandas, VS Code / Jupyter.

---

## 📁 Project Structure

```text
Practical-7/
│
├── data/
│   └── ratings.csv                  # MovieLens-style ratings dataset (1.5M rows)
│
├── output/
│   ├── performance_report.txt       # Auto-generated empirical performance report
│   └── performance_chart.png        # Execution time comparison bar charts
│
├── practical7.py                    # Main PySpark practical implementation script
├── requirements.txt                 # Python dependencies
└── README.md                        # Practical documentation & Viva Q&A guide
```

---

## 📊 Dataset Specification

* **Filename:** `data/ratings.csv`
* **Fields:**
  * `userId` (Integer): Unique ID of the user rating the movie.
  * `movieId` (Integer): Unique ID of the evaluated movie.
  * `rating` (Double): User rating score ranging from `0.5` to `5.0`.
* **Row Count:** 1,500,000 ratings (approx. 20.7 MB CSV).
* **Reproducibility:** Automatically generated using fixed seed `42` if missing.

---

## ⚙️ Installation & Setup

1. **Clone or Navigate to Project Directory:**
   ```bash
   cd Practical-7
   ```

2. **Create and Activate Virtual Environment (Recommended):**
   * **Windows (PowerShell):**
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\activate
     ```
   * **Linux / macOS:**
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Practical:**
   ```bash
   python practical7.py
   ```

---

## 🚀 Execution & Workflow Steps

The execution script (`practical7.py`) performs the following step-by-step tasks:

1. **Spark Session Initialization:** Creates a local SparkSession named `"Practical7_Caching_Partitioning"`.
2. **Dataset Ingestion:** Reads `data/ratings.csv` into a PySpark DataFrame using an explicit `StructType` schema.
3. **Partition Analysis:** Inspects initial partition count (`df.rdd.getNumPartitions()`).
4. **Baseline Execution:** Measures execution time for calculating average ratings per movie on an **uncached** DataFrame using `time.perf_counter()`.
5. **Caching Optimization:** Calls `df.cache()`, triggers materialization explicitly using `df.count()`, and measures execution time on the cached DataFrame.
6. **Partitioning Optimization:** Tests partition counts `2`, `4`, and `8` using `df.repartition(n)` and measures execution time for each.
7. **Multiple Spark Actions Benchmark:** Compares executing 3 separate actions (avg rating, rating filter count, user summary) on Uncached vs Cached DataFrames.
8. **Storage Level Comparison:** Compares `df.cache()` (default `MEMORY_AND_DISK`) against `df.persist(StorageLevel.DISK_ONLY)` (with explicit `unpersist()` cleanup between runs).
9. **Partition Reduction Strategy:** Compares `repartition(2)` (full shuffle) vs `coalesce(2)` (no shuffle partition merging).
10. **Report & Visualization Generation:** Writes empirical metrics to `output/performance_report.txt` and generates `output/performance_chart.png`.

---

## 📈 Measured Performance Summary

| Benchmark Strategy | Partitions | Measured Execution Time | Performance Impact |
| :--- | :---: | :---: | :--- |
| **Baseline (Uncached)** | 6 | **~2.005 s** | Baseline (0.0%) |
| **Cached DataFrame (Run 1)** | 6 | **~0.656 s** | **~67.26% Faster** |
| **Cached DataFrame (Run 2)** | 6 | **~0.625 s** | **~68.80% Faster** |
| **Multi-Action (3 Actions, Uncached)** | 6 | **~2.719 s** | Baseline |
| **Multi-Action (3 Actions, Cached)** | 6 | **~1.305 s** | **~52.01% Faster** |
| **repartition(2) [Full Shuffle]** | 2 | **~2.350 s** | Network Shuffle Overhead |
| **coalesce(2) [No Shuffle]** | 2 | **~0.731 s** | **~3.2x Faster than Repartition** |

---

## 💡 Core Spark Concepts Explained

### 1. Lazy Evaluation & Materialization
In Apache Spark, transformations (e.g. `filter()`, `groupBy()`, `repartition()`) are **lazy**. Spark builds a Directed Acyclic Graph (DAG) of logical execution plans without evaluating data. Calling `df.cache()` only marks the DataFrame for caching in the plan. Materialization only occurs when a Spark **action** (such as `.count()`, `.show()`, or `.collect()`) is called.

### 2. Caching (`df.cache()`) vs Persisting (`df.persist()`)
* `cache()` is a shortcut for `persist(StorageLevel.MEMORY_AND_DISK)`.
* `persist(StorageLevel)` allows selecting custom storage levels:
  * `MEMORY_ONLY`: Stores deserialized RDD/DF objects in RAM.
  * `MEMORY_AND_DISK`: Stores in RAM; spills partitions to disk if memory is insufficient.
  * `DISK_ONLY`: Serializes and writes partitions directly to disk.

### 3. Partitioning: `repartition()` vs `coalesce()`
* **`repartition(n)`:** Can increase or decrease partition count. It performs a **full data shuffle** across cluster nodes to distribute data evenly.
* **`coalesce(n)`:** Used **only to reduce** partition count. It combines adjacent partitions on the same executor **without a full shuffle**, saving significant I/O.

### 4. Catalyst Optimizer
Spark's Catalyst Optimizer compiles high-level DataFrame transformations into optimized physical execution plans (predicate pushdown, column pruning, hash join optimization).

---

## ❓ Viva Voce Questions & Answers (25 Q&As)

1. **What is Apache Spark?**
   * *Answer:* Apache Spark is an open-source, distributed, fast cluster computing framework designed for large-scale data processing and analytics.

2. **What is a Spark DataFrame?**
   * *Answer:* A DataFrame is a distributed, named-column data collection conceptually similar to a relational table, optimized by Spark's Catalyst Optimizer.

3. **What is caching in Spark?**
   * *Answer:* Caching is an optimization technique that keeps computed DataFrames/RDDs in executor memory so subsequent actions reuse intermediate data without recomputation.

4. **Why does caching improve query performance?**
   * *Answer:* Caching avoids repeated disk read I/O, network data transfer, and re-executing the transformation DAG for downstream actions.

5. **Why is `df.cache()` lazy?**
   * *Answer:* `df.cache()` only records the caching intent in Spark's logical plan. It does not load data into RAM until an action is executed.

6. **Why do we call `df.count()` immediately after `df.cache()`?**
   * *Answer:* `df.count()` is a Spark action that forces DAG evaluation, materializing and storing the dataset partitions in cache storage before benchmarking downstream queries.

7. **What is `df.persist()`?**
   * *Answer:* `persist()` is a method that allows developers to specify custom storage levels (such as `MEMORY_ONLY`, `MEMORY_AND_DISK`, or `DISK_ONLY`) for caching.

8. **What is the difference between `cache()` and `persist()`?**
   * *Answer:* `cache()` uses the default storage level (`MEMORY_AND_DISK` for DataFrames), while `persist()` allows passing an explicit `StorageLevel` parameter.

9. **What is `StorageLevel.DISK_ONLY`?**
   * *Answer:* `DISK_ONLY` serializes DataFrame partitions and stores them exclusively on disk, skipping RAM caching.

10. **What is partitioning in Spark?**
    * *Answer:* Partitioning splits a large dataset into smaller logical chunks (partitions) that can be processed concurrently in parallel across executor cores.

11. **What does `repartition(n)` do?**
    * *Answer:* `repartition(n)` redistributes data into `n` partitions by performing a full network shuffle across all executors.

12. **What does `coalesce(n)` do?**
    * *Answer:* `coalesce(n)` reduces partition count by combining existing adjacent partitions on executor nodes without performing a full data shuffle.

13. **What is the main difference between `repartition()` and `coalesce()`?**
    * *Answer:* `repartition()` can increase/decrease partitions and causes a full shuffle; `coalesce()` can only decrease partitions and avoids a full shuffle.

14. **What is lazy evaluation in Spark?**
    * *Answer:* Lazy evaluation means Spark postpones transformation execution until an action is called, allowing the Catalyst Optimizer to optimize the overall DAG.

15. **What is a Spark action? Give examples.**
    * *Answer:* An action triggers physical execution of the transformation DAG and returns a result to the driver or writes to storage (e.g., `count()`, `collect()`, `show()`, `save()`).

16. **What is a Spark transformation? Give examples.**
    * *Answer:* A transformation creates a new RDD/DataFrame from an existing one (e.g., `filter()`, `map()`, `groupBy()`, `select()`).

17. **Why can having too many partitions hurt performance?**
    * *Answer:* Too many small partitions increase task scheduling overhead, driver management burden, and JVM object overhead.

18. **Why can having too few partitions hurt performance?**
    * *Answer:* Too few partitions lead to underutilized CPU cores, high memory pressure per executor task, and potential Out-Of-Memory (OOM) errors.

19. **What is a data shuffle in Spark?**
    * *Answer:* A shuffle is the process of redistributing data across executors, disks, and network connections (triggered by wide transformations like `groupBy()` or `join()`).

20. **Why might caching sometimes NOT improve performance?**
    * *Answer:* If a DataFrame is used only once, if memory is insufficient causing heavy disk spilling, or if dataset size is extremely small, caching overhead can outweigh benefits.

21. **What is Spark's Catalyst Optimizer?**
    * *Answer:* Catalyst is an extensible query optimization engine that analyzes, optimizes, and compiles DataFrame SQL plans into efficient RDD bytecode.

22. **What happens when cached data exceeds available executor memory?**
    * *Answer:* Under `MEMORY_AND_DISK`, excess partitions are spilled to disk. Under `MEMORY_ONLY`, evicted partitions are recalculated from lineage when needed.

23. **Why is partitioning essential for scalability?**
    * *Answer:* Partitioning ensures data is distributed evenly across cluster nodes, enabling linear horizontal scaling as dataset volume grows.

24. **Why should benchmark workloads be identical when comparing techniques?**
    * *Answer:* To ensure scientific validity so timing variations stem strictly from caching or partitioning changes rather than workload differences.

25. **How do you determine the optimal partition count in production?**
    * *Answer:* Target partition sizes between **100 MB and 200 MB**, or set partition count to 2 to 4 times the total available executor CPU cores.

---
