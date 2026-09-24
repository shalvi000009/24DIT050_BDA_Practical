# Practical 3 - MapReduce Word Count Using Hadoop

**Student ID:** 24DIT050  
**Course:** CSUE301 - Big Data Analytics  
**Practical Number:** 3  
**Topic:** MapReduce Word Count Application using Hadoop HDFS  

---

## 1. Problem Definition
A digital publishing company stores thousands of news articles and blog posts in a Hadoop Distributed File System (HDFS). The editorial team wants to identify the most frequently used words across their content to understand trending topics and optimize content recommendations. As a Big Data Engineer, the objective is to develop a MapReduce application that processes text data and generates word frequency statistics.

## 2. Objective
- Implement a MapReduce Word Count application using Hadoop HDFS.
- Understand Mapper and Reducer component workflows for processing unstructured text datasets.
- Process data stored in HDFS and generate word frequency outputs.
- Execute performance evaluation across small, medium, and large dataset sizes to analyze processing time and overhead.
- Perform stop-word filtering and extract Top 10 most frequent words.

## 3. Dataset Description
- **Input Datasets:** Text files containing news articles and editorial documents (`small_text.txt`, `medium_text.txt`, `large_text.txt`).
- **Input Format:** Plain text lines containing space-separated words.
- **Output Format:** Tab-separated key-value pairs (`Word\tCount`).

## 4. Implementation & Technologies
- **Operating System:** Ubuntu Linux (WSL)
- **Hadoop Version:** Apache Hadoop 3.x
- **Framework:** MapReduce / YARN
- **Execution Mode:** Java MapReduce Driver / Python Hadoop Streaming
- **Language / Runtime:** Java JDK / Python 3
- **File System:** HDFS

## 5. Key Analysis & Workflow Executed
1. **Environment Initialization:** Started Hadoop HDFS and YARN daemons (`start-all.sh`) and verified with `jps`.
2. **HDFS Input Preparation:** Created HDFS directory `/wordcount/input` and uploaded sample text files using `hdfs dfs -put`.
3. **Map Phase:** Tokenized incoming text lines into individual words and emitted intermediate `(word, 1)` key-value pairs.
4. **Shuffle & Sort Phase:** Hadoop framework grouped identical word keys together into lists (e.g., `data -> [1, 1, 1]`).
5. **Reduce Phase:** Reducer aggregated the count values for each unique key to output `(word, total_count)`.
6. **Stop-Word & Top 10 Filtering:** Filtered common stop-words (`the`, `is`, `and`, `of`) and sorted word frequencies descending to find the top 10 trending words.
7. **Benchmarking:** Measured YARN job completion times across dataset scale tiers:
   - **Small Dataset:** ~26.65 seconds
   - **Medium Dataset:** ~23.53 seconds
   - **Large Dataset:** ~26.55 seconds

## 6. Key Questions & Analysis (Viva Answers)

**Q1. What is the role of Mapper and Reducer in the Word Count application?**  
- **Mapper:** Reads input lines, tokenizes text into individual words, and generates intermediate key-value pairs formatted as `(word, 1)`.
- **Reducer:** Receives grouped key-value pairs for each unique word key from the Shuffle & Sort phase, sums all count values, and outputs the final `(word, total_frequency)` result.

**Q2. How are intermediate key-value pairs generated and processed?**  
Mapper emits `(word, 1)` for every occurrence. The Hadoop framework executes Shuffle & Sort, bringing identical keys together (e.g., `data -> [1, 1, 1]`). The Reducer receives this grouped list, sums the numbers, and outputs `data\t3`.

**Q3. How does HDFS store and distribute the input dataset?**  
HDFS splits input files into fixed-size data blocks (default 128 MB) and distributes them across cluster DataNodes. The master NameNode manages block mapping metadata. In a single-node setup, storage and computation run locally on the single node.

**Q4. What advantages does MapReduce offer for processing large datasets?**  
MapReduce provides parallel processing, horizontal scalability, automatic fault tolerance (re-executing failed tasks), data locality (processing data where it resides to reduce network I/O), and handling of petabyte-scale data.

**Q5. How does execution time vary with dataset size in this experiment?**  
For small-to-medium datasets, execution time remains nearly constant (~23–26 seconds) because YARN container allocation and JVM startup overhead dominate execution. As data scales into gigabytes/terabytes on multi-node clusters, MapReduce parallel processing benefits outweigh initial startup overhead.

## 7. How to Run / Commands Executed

```bash
# 1. Start Hadoop Cluster
start-dfs.sh
start-yarn.sh

# 2. Upload Input Text Dataset to HDFS
hdfs dfs -mkdir -p /wordcount/input
hdfs dfs -put sample_text.txt /wordcount/input/

# 3. Execute MapReduce Job using Hadoop Streaming
mapred streaming \
  -files mapper.py,reducer.py \
  -mapper mapper.py \
  -reducer reducer.py \
  -input /wordcount/input/sample_text.txt \
  -output /wordcount/output

# 4. View Word Count Results from HDFS
hdfs dfs -cat /wordcount/output/part-00000 | head -n 20

# 5. Extract Top 10 Most Frequent Words
hdfs dfs -cat /wordcount/output/part-00000 | sort -k2 -nr | head -n 10
```
