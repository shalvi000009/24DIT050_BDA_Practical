# Practical 2 - Hadoop Installation and HDFS Operations

**Student ID:** 24DIT050  
**Course:** CSUE301 - Big Data Analytics  
**Practical Number:** 2  
**Topic:** Install and Configure Hadoop in a Single-Node Environment and Perform HDFS File Management & Cluster Monitoring Operations  

---

## 1. Problem Definition
DataAnalytics Inc. processes large-scale customer, sales, and marketing data using the Hadoop ecosystem. Due to the increasing volume of data, the company has decided to migrate its storage infrastructure from traditional file systems to Hadoop Distributed File System (HDFS). As a Hadoop Administrator, the task is to install and configure Hadoop in a single-node cluster environment, verify Hadoop services, manage HDFS storage, and perform file management operations efficiently.

## 2. Objective
- Install and configure Hadoop in a single-node environment.
- Verify Hadoop installation and running cluster daemons (`NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`).
- Perform HDFS file and directory operations (`mkdir`, `put`, `cat`, `cp`, `mv`, `get`, `rm`, `rmdir`).
- Monitor Hadoop cluster health and HDFS storage utilization using `hdfs dfsadmin -report` and `hdfs dfs -du -h`.
- Understand the architecture and inner workings of HDFS.

## 3. Environment & Workspace Description
- **Cluster Mode:** Single-Node Pseudo-Distributed Hadoop Cluster
- **Hadoop Home:** `/usr/local/hadoop` or `~/hadoop`
- **HDFS Root Path:** `hdfs://localhost:9000/`
- **User Directory in HDFS:** `/user/24DIT050/` or `/user/hadoop/`

## 4. Implementation & Technologies
- **Operating System:** Ubuntu Linux (WSL)
- **Hadoop Version:** Apache Hadoop 3.3.6
- **Java Environment:** OpenJDK 11+ / OpenJDK 8
- **Storage Subsystem:** HDFS (Hadoop Distributed File System)
- **Shell Environment:** Bash Terminal
- **Configuration Files:** `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`, `hadoop-env.sh`

## 5. Key Analysis & Workflow Executed
1. **Service Verification:** Checked running Hadoop processes using `jps` to ensure NameNode, DataNode, and YARN daemons were active.
2. **Directory Management:** Created dedicated project directories in HDFS using `hdfs dfs -mkdir -p /bda_data/raw`.
3. **Data Upload:** Transferred local sample datasets into HDFS using `hdfs dfs -put local_sales.csv /bda_data/raw/`.
4. **File Inspection & Manipulation:** Displayed file contents directly from HDFS using `hdfs dfs -cat`, copied files within HDFS using `-cp`, renamed and moved files using `-mv`.
5. **Data Retrieval & Deletion:** Downloaded HDFS files back to the local Linux file system using `hdfs dfs -get` and deleted temporary directories using `-rm -r`.
6. **Cluster Health & Storage Report:** Evaluated total capacity, used space, live DataNodes, and block distribution using `hdfs dfsadmin -report` and `hdfs dfs -du -h`.

## 6. Key Questions & Analysis (Viva Answers)

**Q1. Why is Hadoop preferred for Big Data storage?**  
Hadoop is preferred because it provides distributed, fault-tolerant, highly scalable, and cost-effective storage (HDFS) and processing (MapReduce/YARN) across commodity hardware clusters without requiring expensive high-end servers.

**Q2. What is the role of NameNode and DataNode in HDFS?**  
- **NameNode:** Acts as the master node. It maintains filesystem metadata, block locations, directory trees, and manages client access to files.
- **DataNode:** Acts as worker nodes. It stores the actual data blocks, handles read/write requests from clients, and periodically sends heartbeats and block reports to the NameNode.

**Q3. What is the difference between HDFS and traditional file systems?**

| Feature | HDFS | Traditional File System (NTFS/ext4) |
| :--- | :--- | :--- |
| **Architecture** | Distributed across multiple nodes | Single physical machine |
| **Data Capacity** | Terabytes to Petabytes | Gigabytes to Terabytes |
| **Fault Tolerance** | High (Block Replication) | Limited (Single point of failure) |
| **Scalability** | Horizontal (Add more nodes) | Vertical (Upgrade hardware) |
| **Optimized For** | Streaming access to large files | Random read/write of small files |

**Q4. What are the advantages of distributed storage?**  
Distributed storage provides high availability, automatic fault tolerance through data block replication, seamless horizontal scalability, faster parallel read/write performance, and reliable data retention.

## 7. How to Run / Commands Executed

```bash
# 1. Start Hadoop Services and Verify Daemons
start-dfs.sh
start-yarn.sh
jps

# 2. Check HDFS Cluster Report
hdfs dfsadmin -report

# 3. Create Directories in HDFS
hdfs dfs -mkdir -p /user/24DIT050/bda_input

# 4. Upload Dataset from Local to HDFS
hdfs dfs -put sales_data.csv /user/24DIT050/bda_input/

# 5. List and View File Contents in HDFS
hdfs dfs -ls /user/24DIT050/bda_input/
hdfs dfs -cat /user/24DIT050/bda_input/sales_data.csv

# 6. Copy, Move, and Download Files
hdfs dfs -cp /user/24DIT050/bda_input/sales_data.csv /user/24DIT050/sales_backup.csv
hdfs dfs -get /user/24DIT050/sales_backup.csv ./downloaded_sales.csv

# 7. Check Disk Usage and Storage Utilization
hdfs dfs -du -h /user/24DIT050/

# 8. Clean up Files in HDFS
hdfs dfs -rm /user/24DIT050/sales_backup.csv
```
