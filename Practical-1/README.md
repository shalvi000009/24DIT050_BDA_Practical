# Practical 1 - Basic Linux Commands and File Management

**Student ID:** 24DIT050  
**Course:** CSUE301 - Big Data Analytics  
**Practical Number:** 1  
**Topic:** Basic Linux Commands for System Information, File & Directory Management, Searching, Permissions, Backups, and System Monitoring  

---

## 1. Problem Definition
You have joined Data Analytics Inc. as a Linux System Administrator. Your task is to set up the Marketing Analytics project environment by creating directories, managing files, monitoring system resources, searching data, and creating backups for the Marketing Analytics team.

## 2. Objective
- Identify the current user (`whoami`) and working directory (`pwd`).
- Display Linux system information (`uname -a`, `uptime`, `date`).
- Create and manage project directory structures (`MarketingAnalytics`, `RawData`, `ProcessedData`, `Reports`, `Backup`).
- Add and view file contents (`touch`, `cat`, `nano`, `head`, `tail`).
- Search files and text using Linux commands (`find`, `grep`).
- Perform file management operations: copy (`cp`), rename/move (`mv`), and delete (`rm`, `rmdir`).
- Create automated backups and monitor system resource utilization (`top`, `htop`, `du`, `df`, `cron`).

## 3. Environment & Workspace Description
- **Project Root Directory:** `MarketingAnalytics/`
- **Subdirectories:**
  - `RawData/` - Contains raw incoming dataset files.
  - `ProcessedData/` - Contains cleaned and transformed analytical datasets.
  - `Reports/` - Contains generated marketing insights and summary text files.
  - `Backup/` - Archive directory for system and report backups.

## 4. Implementation & Technologies
- **Operating System:** Ubuntu Linux (WSL)
- **Shell Environment:** Bash Shell / Linux Terminal
- **Editor:** Nano Text Editor
- **Task Scheduler:** Cron
- **Core Linux Commands:** `whoami`, `pwd`, `ls`, `mkdir`, `touch`, `cat`, `grep`, `find`, `cp`, `mv`, `rm`, `rmdir`, `du`, `df`, `top`

## 5. Key Analysis & Workflow Executed
1. **User & Directory Identification:** Verified logged-in user and root working path using `whoami` and `pwd`.
2. **System Health Verification:** Checked OS details, system uptime, and current system timestamp using `uname -a`, `uptime`, and `date`.
3. **Environment Setup:** Created structured directory hierarchy using `mkdir -p MarketingAnalytics/{RawData,ProcessedData,Reports,Backup}`.
4. **File Creation & Inspection:** Populated sample datasets and reports using `touch` and `nano`; inspected contents with `cat`, `head`, and `tail`.
5. **Pattern & File Search:** Filtered specific log patterns using `grep -i "keyword" filename` and located files across directories using `find . -name "*.txt"`.
6. **File Operations & Maintenance:** Copied reports to `Backup/`, renamed project files, moved processed files, and removed temporary files using `cp`, `mv`, and `rm`.
7. **Resource Monitoring & Automation:** Monitored real-time CPU/RAM usage with `top`, analyzed disk consumption with `du -sh *` and `df -h`, and configured automated backup tasks using `crontab`.

## 6. Key Questions & Analysis (Viva Answers)

**Q1. What is the difference between absolute and relative paths?**  
- **Absolute Path:** Starts from the root directory (`/`) and specifies the complete location of a file or directory (e.g., `/home/user/MarketingAnalytics/Reports`).
- **Relative Path:** Starts from the current working directory and specifies a location relative to where the user is currently positioned (e.g., `./Reports`).

**Q2. Why are Linux permissions important for file security?**  
Linux file permissions (read `r`, write `w`, execute `x`) protect sensitive system and project files from unauthorized modification or execution, enforce user-level access controls, and maintain system data integrity.

**Q3. What is the purpose of the grep command?**  
The `grep` (Global Regular Expression Print) command searches for specific text strings or regular expression patterns inside files and filters output lines from command pipelines.

**Q4. How does Linux support efficient file management?**  
Linux provides a structured hierarchical tree directory model, powerful CLI utilities (`ls`, `cp`, `mv`, `rm`, `find`, `grep`), fine-grained file permissions, and shell scripting capabilities for automated batch operations.

**Q5. Why are backups important in enterprise environments?**  
Backups prevent data loss caused by hardware failure, accidental deletion, or malware, support quick disaster recovery, and ensure enterprise business continuity.

## 7. How to Run / Commands Executed

```bash
# 1. Identify User and Working Directory
whoami
pwd

# 2. Display System Information and Status
uname -a
uptime
date

# 3. Create Project Directory Structure
mkdir -p MarketingAnalytics/RawData
mkdir -p MarketingAnalytics/ProcessedData
mkdir -p MarketingAnalytics/Reports
mkdir -p MarketingAnalytics/Backup
cd MarketingAnalytics

# 4. Create and View Sample Data Files
touch RawData/sales_raw.csv Reports/q1_report.txt
echo "Order_ID,Product,Amount" > RawData/sales_raw.csv
echo "101,Laptop,55000" >> RawData/sales_raw.csv
cat RawData/sales_raw.csv

# 5. List Files and Search Content
ls -la Reports/
grep -i "Laptop" RawData/sales_raw.csv
find . -name "*.txt"

# 6. Copy, Rename, and Move Files
cp Reports/q1_report.txt Backup/
mv Reports/q1_report.txt Reports/q1_summary_2026.txt

# 7. Monitor Resources and Disk Usage
du -sh *
df -h
top -b -n 1
```
