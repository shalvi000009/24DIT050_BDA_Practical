"""
================================================================================
Big Data Analytics - Practical 7: Performance Optimization Using Caching & Partitioning
================================================================================
Author: 24DIT050
Technology: Apache Spark 3.x, PySpark, Python 3.11

Scenario:
MovieFlix Technologies operates a movie recommendation platform processing large-scale
movie rating data. As data volumes expand, repeated aggregation queries cause performance
degradation. This practical demonstrates performance optimization using:
  1. Data Caching (RAM / Disk persistence)
  2. DataFrame Partitioning (repartition & coalesce)
  3. Execution Time Benchmarking across multiple Spark actions
================================================================================
"""

import os
import sys
import time
import random
import csv

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.storagelevel import StorageLevel


# ------------------------------------------------------------------------------
# HELPER: Dataset Generation
# ------------------------------------------------------------------------------
def generate_dataset_if_needed(file_path: str, num_rows: int = 1500000) -> None:
    """
    Generates a synthetic MovieLens-style ratings CSV dataset if not present.
    Uses a fixed random seed (42) for reproducibility.

    Schema: userId (int), movieId (int), rating (float)
    """
    if os.path.exists(file_path):
        print(f"[DATASET] Dataset already exists at '{file_path}'. Skipping generation.")
        return

    print(f"[DATASET] Generating reproducible sample ratings dataset with {num_rows:,} rows...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    random.seed(42)
    users = list(range(1, 10001))      # 10,000 unique users
    movies = list(range(1, 2501))      # 2,500 unique movies
    possible_ratings = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]

    start_time = time.perf_counter()
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["userId", "movieId", "rating"])
        
        # Batch write for high performance
        batch_size = 100000
        rows = []
        for i in range(1, num_rows + 1):
            user_id = random.choice(users)
            movie_id = random.choice(movies)
            rating = random.choice(possible_ratings)
            rows.append((user_id, movie_id, rating))

            if len(rows) >= batch_size:
                writer.writerows(rows)
                rows = []

        if rows:
            writer.writerows(rows)

    elapsed = time.perf_counter() - start_time
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"[DATASET] Successfully generated '{file_path}' ({file_size_mb:.2f} MB) in {elapsed:.2f} seconds.")


# ------------------------------------------------------------------------------
# 1. CREATE SPARK SESSION
# ------------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Initializes a local SparkSession configured for practical benchmarks.
    """
    print("\n" + "=" * 80)
    print("STEP 1: CREATING SPARK SESSION")
    print("=" * 80)
    
    spark = (
        SparkSession.builder
        .appName("Practical7_Caching_Partitioning")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    
    # Set log level to WARN to avoid clutter in terminal output
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Session Created Successfully!")
    print(f"Spark Version  : {spark.version}")
    print(f"Master App Name: {spark.sparkContext.appName}")
    return spark


# ------------------------------------------------------------------------------
# 2. LOAD DATASET
# ------------------------------------------------------------------------------
def load_ratings_data(spark: SparkSession, file_path: str):
    """
    Loads ratings dataset into Spark DataFrame with explicit schema.
    """
    print("\n" + "=" * 80)
    print("STEP 2: LOADING DATASET INTO SPARK DATAFRAME")
    print("=" * 80)

    schema = StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", DoubleType(), False)
    ])

    df = spark.read.csv(file_path, header=True, schema=schema)
    
    print("\n--- DataFrame Schema ---")
    df.printSchema()

    print("\n--- Sample Records (First 5 Rows) ---")
    df.show(5, truncate=False)

    row_count = df.count()
    print(f"Total Row Count: {row_count:,} records")
    return df


# ------------------------------------------------------------------------------
# 3. CHECK INITIAL PARTITIONS
# ------------------------------------------------------------------------------
def check_initial_partitions(df):
    """
    Inspects initial partition count of DataFrame.
    """
    print("\n" + "=" * 80)
    print("STEP 3: INITIAL PARTITION ANALYSIS")
    print("=" * 80)

    initial_partitions = df.rdd.getNumPartitions()
    print(f"Initial Number of Partitions: {initial_partitions}")
    print("Explanation: The initial partition count is determined by Spark's file reader based on")
    print("file size and default block split rules (e.g. spark.sql.files.maxPartitionBytes).")
    return initial_partitions


# ------------------------------------------------------------------------------
# WORKLOAD BENCHMARK FUNCTION
# ------------------------------------------------------------------------------
def run_analytical_workload(df, description: str = "Average Rating Calculation") -> float:
    """
    Executes an analytical workload: calculates average ratings and count per movie,
    filters top-rated movies, and triggers action to measure execution time.
    """
    start_time = time.perf_counter()
    
    # Transformation lineage
    result_df = (
        df.groupBy("movieId")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("rating").alias("total_ratings")
        )
        .filter(F.col("avg_rating") >= 2.75)
        .orderBy(F.desc("avg_rating"))
    )
    
    # Trigger Spark Action (count forces execution of DAG)
    computed_count = result_df.count()
    elapsed_time = time.perf_counter() - start_time
    
    print(f"  |- [{description}] Computed {computed_count} movies matching criteria in {elapsed_time:.4f} seconds.")
    return elapsed_time


# ------------------------------------------------------------------------------
# 4. BASELINE PERFORMANCE
# ------------------------------------------------------------------------------
def benchmark_baseline(df) -> float:
    """
    Measures baseline execution time without caching or custom partitioning.
    """
    print("\n" + "=" * 80)
    print("STEP 4: BASELINE PERFORMANCE (UNCACHED)")
    print("=" * 80)

    # Warmup run to avoid JVM compilation overhead affecting results
    _ = df.groupBy("movieId").agg(F.avg("rating")).count()

    print("Running baseline calculation (Uncached DataFrame)...")
    baseline_time = run_analytical_workload(df, "Baseline Uncached Run")
    print(f"Baseline Execution Time: {baseline_time:.4f} seconds")
    return baseline_time


# ------------------------------------------------------------------------------
# 5. CACHING OPTIMIZATION
# ------------------------------------------------------------------------------
def benchmark_caching(df, baseline_time: float) -> tuple[float, float, float]:
    """
    Applies df.cache(), triggers materialization with count(), and benchmarks cached performance.
    """
    print("\n" + "=" * 80)
    print("STEP 5: CACHING OPTIMIZATION")
    print("=" * 80)

    print("Applying df.cache()...")
    # Note: df.cache() is LAZY. It adds a InMemoryRelation node to logical plan but does not evaluate data.
    df.cache()

    print("Triggering cache materialization using df.count()...")
    mat_start = time.perf_counter()
    total_records = df.count()
    mat_time = time.perf_counter() - mat_start
    print(f"Cache Materialized for {total_records:,} records in {mat_time:.4f} seconds.")

    print("\nExecuting analytical workload on CACHED DataFrame...")
    cached_time = run_analytical_workload(df, "Cached Run 1")
    
    # Second run on cached data to demonstrate warm memory access
    cached_time_run2 = run_analytical_workload(df, "Cached Run 2")

    improvement_pct = ((baseline_time - cached_time) / baseline_time) * 100 if baseline_time > 0 else 0
    print(f"\n--- Caching Benchmark Summary ---")
    print(f"Baseline Time (Uncached) : {baseline_time:.4f} seconds")
    print(f"Cached Time (Run 1)      : {cached_time:.4f} seconds")
    print(f"Cached Time (Run 2)      : {cached_time_run2:.4f} seconds")
    print(f"Performance Improvement  : {improvement_pct:.2f}%")

    return cached_time, cached_time_run2, improvement_pct


# ------------------------------------------------------------------------------
# 6. PARTITIONING OPTIMIZATION
# ------------------------------------------------------------------------------
def benchmark_partitioning(df, partition_counts: list = [2, 4, 8]) -> dict:
    """
    Repartitions DataFrame into 2, 4, and 8 partitions and measures workload performance.
    """
    print("\n" + "=" * 80)
    print("STEP 6: PARTITIONING OPTIMIZATION (repartition)")
    print("=" * 80)

    # Ensure we use uncached DF for clean partitioning tests
    df.unpersist()

    partition_results = {}
    for num_p in partition_counts:
        print(f"\n[Partitioning] Repartitioning dataset into {num_p} partitions...")
        df_repartitioned = df.repartition(num_p)
        actual_p = df_repartitioned.rdd.getNumPartitions()
        print(f"  Verified Partition Count: {actual_p}")

        exec_time = run_analytical_workload(df_repartitioned, f"Repartition {num_p}")
        partition_results[num_p] = exec_time

    return partition_results


# ------------------------------------------------------------------------------
# 8. MULTIPLE SPARK ACTIONS
# ------------------------------------------------------------------------------
def demonstrate_multiple_actions(df) -> tuple[float, float]:
    """
    Demonstrates performance when multiple actions reuse the same DataFrame.
    Compares Uncached vs Cached execution across 3 different analytical actions.
    """
    print("\n" + "=" * 80)
    print("STEP 8: MULTIPLE SPARK ACTIONS DEMONSTRATION")
    print("=" * 80)

    # 1. Uncached Run for 3 Actions
    df.unpersist()
    print("--- Running 3 Spark Actions on UNCACHED DataFrame ---")
    start_uncached = time.perf_counter()
    
    # Action 1: Average Rating per Movie
    a1_res = df.groupBy("movieId").agg(F.avg("rating").alias("avg_r")).count()
    print(f"  Action 1 (Avg Rating by Movie) Count: {a1_res}")

    # Action 2: Highly Rated Filter Count
    a2_res = df.filter(F.col("rating") >= 4.0).count()
    print(f"  Action 2 (Rating >= 4.0 Filter) Count: {a2_res}")

    # Action 3: User Rating Summary (Max & Min per User)
    a3_res = df.groupBy("userId").agg(F.max("rating"), F.min("rating")).count()
    print(f"  Action 3 (User Rating Summary) Count: {a3_res}")

    total_uncached_time = time.perf_counter() - start_uncached
    print(f"Total Time for 3 Actions (UNCACHED): {total_uncached_time:.4f} seconds")

    # 2. Cached Run for 3 Actions
    print("\n--- Running 3 Spark Actions on CACHED DataFrame ---")
    df.cache()
    df.count() # Materialize

    start_cached = time.perf_counter()
    
    a1_res_c = df.groupBy("movieId").agg(F.avg("rating").alias("avg_r")).count()
    print(f"  Action 1 (Avg Rating by Movie) Count: {a1_res_c}")

    a2_res_c = df.filter(F.col("rating") >= 4.0).count()
    print(f"  Action 2 (Rating >= 4.0 Filter) Count: {a2_res_c}")

    a3_res_c = df.groupBy("userId").agg(F.max("rating"), F.min("rating")).count()
    print(f"  Action 3 (User Rating Summary) Count: {a3_res_c}")

    total_cached_time = time.perf_counter() - start_cached
    print(f"Total Time for 3 Actions (CACHED): {total_cached_time:.4f} seconds")

    improvement = ((total_uncached_time - total_cached_time) / total_uncached_time) * 100
    print(f"\nMulti-Action Time Savings with Cache: {improvement:.2f}% faster")

    df.unpersist()
    return total_uncached_time, total_cached_time


# ------------------------------------------------------------------------------
# 9. CACHE() VS PERSIST(STORAGELEVEL.DISK_ONLY)
# ------------------------------------------------------------------------------
def compare_cache_and_persist(df) -> tuple[float, float]:
    """
    Compares cache() (MEMORY_AND_DISK default) with persist(StorageLevel.DISK_ONLY).
    """
    print("\n" + "=" * 80)
    print("STEP 9: CACHE() VS PERSIST(StorageLevel.DISK_ONLY)")
    print("=" * 80)

    # Clean unpersist
    df.unpersist()

    # 1. Test cache() [MEMORY_AND_DISK]
    print("Testing df.cache() (Default: MEMORY_AND_DISK)...")
    df.cache()
    df.count() # materialize
    time_cache = run_analytical_workload(df, "cache() Workload")
    df.unpersist()

    # 2. Test persist(StorageLevel.DISK_ONLY)
    print("\nTesting df.persist(StorageLevel.DISK_ONLY)...")
    df.persist(StorageLevel.DISK_ONLY)
    df.count() # materialize
    time_disk = run_analytical_workload(df, "persist(DISK_ONLY) Workload")
    df.unpersist()

    print(f"\n--- Cache vs Persist Summary ---")
    print(f"cache() [MEMORY_AND_DISK] Time : {time_cache:.4f} seconds")
    print(f"persist(DISK_ONLY) Time        : {time_disk:.4f} seconds")
    print("Explanation: MEMORY_AND_DISK reads cached data directly from RAM when memory permits.")
    print("DISK_ONLY serializes and reads partitions from disk storage, incurring disk I/O overhead.")

    return time_cache, time_disk


# ------------------------------------------------------------------------------
# 10. REPARTITION() VS COALESCE()
# ------------------------------------------------------------------------------
def compare_repartition_and_coalesce(df) -> tuple[float, float]:
    """
    Compares repartition(2) vs coalesce(2) when reducing partitions from 8 to 2.
    """
    print("\n" + "=" * 80)
    print("STEP 10: REPARTITION() VS COALESCE()")
    print("=" * 80)

    df.unpersist()
    # Start with 8 partitions
    df_8 = df.repartition(8)
    df_8.cache()
    df_8.count() # materialize 8 partitions

    print("Reducing partitions from 8 to 2 using repartition(2)...")
    df_repart = df_8.repartition(2)
    time_repart = run_analytical_workload(df_repart, "repartition(2) Workload")

    print("\nReducing partitions from 8 to 2 using coalesce(2)...")
    df_coalesce = df_8.coalesce(2)
    time_coalesce = run_analytical_workload(df_coalesce, "coalesce(2) Workload")

    print(f"\n--- Repartition vs Coalesce Summary ---")
    print(f"repartition(2) Execution Time : {time_repart:.4f} seconds")
    print(f"coalesce(2) Execution Time    : {time_coalesce:.4f} seconds")
    print("Explanation: repartition(n) performs a full data shuffle across all executor nodes.")
    print("coalesce(n) reduces partitions by combining existing adjacent partitions without full shuffle.")

    df_8.unpersist()
    return time_repart, time_coalesce


# ------------------------------------------------------------------------------
# 11. GENERATE PERFORMANCE REPORT
# ------------------------------------------------------------------------------
def generate_performance_report(
    output_path: str,
    row_count: int,
    initial_partitions: int,
    baseline_time: float,
    cached_time: float,
    cached_time_run2: float,
    partition_results: dict,
    multi_uncached: float,
    multi_cached: float,
    time_cache: float,
    time_disk: float,
    time_repart: float,
    time_coalesce: float
):
    """
    Generates structured output/performance_report.txt detailing all empirical metrics.
    """
    print("\n" + "=" * 80)
    print("STEP 11: GENERATING PERFORMANCE REPORT")
    print("=" * 80)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    improvement_pct = ((baseline_time - cached_time) / baseline_time) * 100 if baseline_time > 0 else 0
    multi_improvement = ((multi_uncached - multi_cached) / multi_uncached) * 100 if multi_uncached > 0 else 0

    report_content = f"""================================================================================
SPARK PERFORMANCE OPTIMIZATION REPORT: CACHING & PARTITIONING
================================================================================
Practical  : Practical 7 - Performance Optimization Using Caching & Partitioning
Application: MovieFlix Technologies Movie Rating Analytics
Date       : 2026-09-29
Environment: Apache Spark (Local Mode), PySpark, Python 3.11

1. EXECUTIVE SUMMARY
--------------------------------------------------------------------------------
This report presents the empirical benchmark results of optimizing a Spark rating
aggregation pipeline using DataFrame Caching, Storage Level selection, and
DataFrame Partitioning strategies (repartition vs coalesce).

Dataset Details:
  - Total Ratings Processed : {row_count:,} records
  - Initial Partition Count : {initial_partitions}

2. PERFORMANCE BENCHMARK SUMMARY TABLE
--------------------------------------------------------------------------------
Technique / Strategy       | Partitions | Execution Time (s) | Improvement vs Baseline
--------------------------------------------------------------------------------
Baseline (Uncached)        | {initial_partitions:<10} | {baseline_time:<18.4f} | Base (0.0%)
Cached DataFrame (Run 1)   | {initial_partitions:<10} | {cached_time:<18.4f} | {improvement_pct:+.2f}%
Cached DataFrame (Run 2)   | {initial_partitions:<10} | {cached_time_run2:<18.4f} | {((baseline_time - cached_time_run2)/baseline_time)*100:+.2f}%
Repartitioned (2 Partitions)| 2          | {partition_results.get(2, 0.0):<18.4f} | {((baseline_time - partition_results.get(2, 0.0))/baseline_time)*100:+.2f}%
Repartitioned (4 Partitions)| 4          | {partition_results.get(4, 0.0):<18.4f} | {((baseline_time - partition_results.get(4, 0.0))/baseline_time)*100:+.2f}%
Repartitioned (8 Partitions)| 8          | {partition_results.get(8, 0.0):<18.4f} | {((baseline_time - partition_results.get(8, 0.0))/baseline_time)*100:+.2f}%
--------------------------------------------------------------------------------

3. DETAILED EXPERIMENTAL ANALYSIS
--------------------------------------------------------------------------------
A. Baseline vs. Cached Execution:
   - Baseline Uncached Time : {baseline_time:.4f} seconds
   - Cached Execution Time   : {cached_time:.4f} seconds
   - Speedup / Improvement   : {improvement_pct:.2f}%
   - Observations:
     * Caching avoids re-reading data from disk and re-evaluating transform DAGs.
     * Caching is lazy: df.cache() does not execute until an action (e.g. count()) is called.

B. Multiple Spark Actions Optimization:
   - Total Time for 3 Actions (UNCACHED): {multi_uncached:.4f} seconds
   - Total Time for 3 Actions (CACHED)  : {multi_cached:.4f} seconds
   - Multi-Action Time Savings          : {multi_improvement:.2f}% faster
   - Takeaway: When a dataset is reused across multiple actions (avg rating, count, max/min),
     caching provides exponential time savings by materializing intermediate results.

C. Storage Levels (cache vs persist(DISK_ONLY)):
   - df.cache() [MEMORY_AND_DISK]     : {time_cache:.4f} seconds
   - df.persist(DISK_ONLY)            : {time_disk:.4f} seconds
   - Analysis: MEMORY_AND_DISK keeps data deserialized in RAM where possible. DISK_ONLY
     serializes data and reads from local disk, incurring disk I/O cost.

D. Partitioning Techniques (repartition vs coalesce):
   - repartition(2) Time (Full Shuffle): {time_repart:.4f} seconds
   - coalesce(2) Time (No Shuffle)    : {time_coalesce:.4f} seconds
   - Analysis: coalesce(n) avoids a full network shuffle when reducing partition count,
     making it substantially more efficient than repartition(n) for partition reduction.

4. CONCLUSION & RECOMMENDATIONS
--------------------------------------------------------------------------------
1. Always cache DataFrames when performing multiple downstream actions.
2. Trigger cache materialization explicitly with an action (e.g., .count()).
3. Use coalesce() instead of repartition() when reducing partition counts.
4. Choose appropriate partition sizes (aim for 100MB - 200MB per partition in production).
================================================================================
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"Performance report saved successfully to '{output_path}'.")


# ------------------------------------------------------------------------------
# 12. GENERATE PERFORMANCE CHART (OPTIONAL)
# ------------------------------------------------------------------------------
def generate_performance_chart(
    chart_path: str,
    baseline_time: float,
    cached_time: float,
    partition_results: dict,
    time_repart: float,
    time_coalesce: float
):
    """
    Generates a visual performance comparison chart using matplotlib if available.
    """
    try:
        import matplotlib.pyplot as plt

        os.makedirs(os.path.dirname(chart_path), exist_ok=True)
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        fig.suptitle("Spark Performance Optimization Benchmarks", fontsize=14, fontweight="bold")

        # Chart 1: Baseline vs Cached vs Repartitioning
        categories = ["Baseline", "Cached", "Partition 2", "Partition 4", "Partition 8"]
        times = [
            baseline_time,
            cached_time,
            partition_results.get(2, 0.0),
            partition_results.get(4, 0.0),
            partition_results.get(8, 0.0)
        ]
        colors = ["#e74c3c", "#2ecc71", "#3498db", "#9b59b6", "#f1c40f"]

        axes[0].bar(categories, times, color=colors, edgecolor="black", linewidth=0.8)
        axes[0].set_title("Execution Time by Strategy (lower is better)")
        axes[0].set_ylabel("Execution Time (seconds)")
        axes[0].grid(axis="y", linestyle="--", alpha=0.7)

        for i, v in enumerate(times):
            axes[0].text(i, v + (max(times)*0.01), f"{v:.3f}s", ha="center", fontweight="bold")

        # Chart 2: Repartition vs Coalesce
        methods = ["repartition(2)\n(Full Shuffle)", "coalesce(2)\n(No Shuffle)"]
        method_times = [time_repart, time_coalesce]
        axes[1].bar(methods, method_times, color=["#e67e22", "#1abc9c"], edgecolor="black", linewidth=0.8)
        axes[1].set_title("Partition Reduction: repartition vs coalesce")
        axes[1].set_ylabel("Execution Time (seconds)")
        axes[1].grid(axis="y", linestyle="--", alpha=0.7)

        for i, v in enumerate(method_times):
            axes[1].text(i, v + (max(method_times)*0.01), f"{v:.3f}s", ha="center", fontweight="bold")

        plt.tight_layout()
        plt.savefig(chart_path, dpi=300)
        plt.close()
        print(f"Performance chart saved successfully to '{chart_path}'.")

    except ImportError:
        print("[CHART] Matplotlib is not installed. Skipping PNG chart generation.")
    except Exception as e:
        print(f"[CHART] Note on chart generation: {e}")


# ------------------------------------------------------------------------------
# MAIN EXECUTION FLOW
# ------------------------------------------------------------------------------
def main():
    print("=" * 80)
    print("PRACTICAL 7: PERFORMANCE OPTIMIZATION USING CACHING AND PARTITIONING")
    print("=" * 80)

    dataset_path = os.path.join("data", "ratings.csv")
    report_path = os.path.join("output", "performance_report.txt")
    chart_path = os.path.join("output", "performance_chart.png")

    # Generate dataset if needed
    generate_dataset_if_needed(dataset_path, num_rows=1500000)

    # 1. Create Spark Session
    spark = create_spark_session()

    try:
        # 2. Load Dataset
        df = load_ratings_data(spark, dataset_path)
        row_count = df.count()

        # 3. Check Initial Partitions
        initial_partitions = check_initial_partitions(df)

        # 4. Baseline Performance (Uncached)
        baseline_time = benchmark_baseline(df)

        # 5. Caching Benchmark
        cached_time, cached_time_run2, improvement_pct = benchmark_caching(df, baseline_time)

        # 6. Partitioning Benchmark
        partition_results = benchmark_partitioning(df, [2, 4, 8])

        # 8. Multiple Spark Actions Demonstration
        multi_uncached, multi_cached = demonstrate_multiple_actions(df)

        # 9. cache() vs persist(DISK_ONLY)
        time_cache, time_disk = compare_cache_and_persist(df)

        # 10. repartition() vs coalesce()
        time_repart, time_coalesce = compare_repartition_and_coalesce(df)

        # 11. Generate Report
        generate_performance_report(
            report_path,
            row_count,
            initial_partitions,
            baseline_time,
            cached_time,
            cached_time_run2,
            partition_results,
            multi_uncached,
            multi_cached,
            time_cache,
            time_disk,
            time_repart,
            time_coalesce
        )

        # 12. Generate Chart
        generate_performance_chart(
            chart_path,
            baseline_time,
            cached_time,
            partition_results,
            time_repart,
            time_coalesce
        )

        print("\n" + "=" * 80)
        print("PRACTICAL 7 EXECUTION COMPLETED SUCCESSFULLY!")
        print(f"Performance report available at: [performance_report.txt](file:///{os.path.abspath(report_path)})")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
