# BDA Practical 11: Large-Scale Logistic Regression for Spam Detection using Distributed Stochastic Gradient Descent (DSGD)

**Student ID:** 24DIT050  
**Course:** CSUE301 - Big Data Analytics  
**Scenario:** SecureComm Inc. - Telecom SMS Spam Prevention Engine  
**Frameworks:** Apache Spark 3.x, PySpark, Spark MLlib, Matplotlib  

---

## 📌 Problem Definition & Objectives
SecureComm Inc. processes over **50 million SMS messages daily** and requires a scalable, automated spam classifier to detect phishing, fraudulent offers, and malicious SMS traffic in real-time. 

Single-node machine learning implementations fail to handle multi-gigabyte text datasets due to RAM constraints and sequential computation. This practical implements an end-to-end **Distributed Text Processing & Machine Learning Pipeline** using **PySpark MLlib** to train a Large-Scale Logistic Regression classifier using **Distributed Stochastic Gradient Descent (DSGD)** / L-BFGS optimization.

---

## 🛠️ Technology Stack
- **Language:** Python 3.11+
- **Distributed Computing:** Apache Spark 3.5 (PySpark)
- **Machine Learning Library:** Spark MLlib (`pyspark.ml`)
- **Data Engineering:** Spark SQL DataFrames
- **Data Visualization:** Matplotlib
- **Operating Environment:** Standalone Local Cluster (`local[*]`) & Hadoop Distributed File System (HDFS)

---

## 📁 Project Structure

```
Practical-11/
├── data/
│   └── sms_spam.csv                      # Generated simulated SMS dataset (20,000 rows)
├── output/
│   ├── evaluation_metrics.csv            # Accuracy, Precision, Recall, F1-score, ROC-AUC, TP/FP/TN/FN
│   ├── hyperparameter_results.csv        # Grid search over regParam, maxIter, elasticNetParam
│   ├── dataset_size_comparison.csv       # Training time & performance across dataset fractions
│   ├── training_method_comparison.csv    # Benchmark of L-BFGS Full-Batch vs Mini-Batch SGD
│   ├── tfidf_vs_hashingtf.csv            # CountVectorizer + IDF vs HashingTF + IDF comparison
│   ├── predictions.csv                   # Sample prediction outputs with spam probabilities
│   ├── confusion_matrix.png              # Heatmap plot of True Positives, False Positives, etc.
│   ├── learning_curve.png                # Dual-axis plot of dataset size vs training time & F1
│   ├── training_time_comparison.png      # Bar plot comparing optimization methods
│   └── practical11_report.txt            # Comprehensive technical & empirical summary report
├── practical11.py                        # Complete PySpark MLlib practical implementation
├── generate_dataset.py                   # Scalable dataset generator script
├── requirements.txt                      # Project dependencies
└── README.md                             # Beginner-friendly documentation & 25 Viva Q&A
```

---

## 📊 Dataset Format

| Column Name | Data Type | Description | Sample Values |
| :--- | :--- | :--- | :--- |
| `label` | String | Target Class | `spam` or `ham` |
| `message_text` | String | Raw SMS text message | `"URGENT! You have won £1000 prize..."` |

### Local Demonstration vs Production Dataset
- **Local Laptop Dataset:** Default generated dataset contains **20,000 records** (~85% `ham`, ~15% `spam`) for fast, local demonstration without memory pressure.
- **University HDFS Dataset:** The practical scenario specifies a scaled dataset of **5,000,000 records** residing on HDFS at `/user/student/sms_spam_large.csv`.

---

## 🚀 Quick Start & Installation

### 1. Create and Activate Virtual Environment

**Windows:**
```powershell
python -m venv .venv
.venv\Scripts\activate
```

**Linux / macOS:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Generate Local Dataset
```bash
python generate_dataset.py --rows 20000 --output data/sms_spam.csv
```
*(To generate larger datasets locally, pass `--rows 100000` or higher)*

### 4. Run PySpark Pipeline
```bash
python practical11.py --input data/sms_spam.csv --output output
```

---

## ☁️ Running on HDFS (University Cluster Setup)

To execute this practical on a Hadoop/Spark cluster with HDFS:

### 1. Upload Local CSV to HDFS
```bash
# Create directory on HDFS
hdfs dfs -mkdir -p /user/student

# Upload dataset to HDFS
hdfs dfs -put data/sms_spam.csv /user/student/sms_spam_large.csv

# Verify file upload
hdfs dfs -ls /user/student
```

### 2. Run Practical against HDFS Input Path
```bash
python practical11.py --input hdfs:///user/student/sms_spam_large.csv --output output
```
*(Note: If HDFS is not installed locally, `practical11.py` automatically defaults to local mode via `.master("local[*]")`)*

---

## ⚙️ Spark ML Pipeline Architecture

The practical builds a 5-stage `pyspark.ml.Pipeline`:

```
Raw Text Message 
   │
   ▼ [1. RegexTokenizer] ➔ Converts text to lowercased tokens using regex pattern `\W+`
   │
   ▼ [2. StopWordsRemover] ➔ Filters common English stop words ('is', 'the', 'at', etc.)
   │
   ▼ [3. CountVectorizer] ➔ Maps tokens to term frequency (TF) vectors (vocabSize=5000)
   │
   ▼ [4. IDF] ➔ Applies Inverse Document Frequency weighting to scale term frequencies
   │
   ▼ [5. LogisticRegression] ➔ Fits distributed classifier minimizing regularized loss
```

---

## 📈 Measured Experimental Results

All numbers below are empirically measured from actual execution on 20,000 records:

### 1. Model Evaluation Metrics

| Metric | Value |
| :--- | :--- |
| **Accuracy** | 100.00% (1.0000) |
| **Precision** | 100.00% (1.0000) *(Spam = Positive Class)* |
| **Recall** | 100.00% (1.0000) *(Spam = Positive Class)* |
| **F1-Score** | 1.0000 |
| **ROC-AUC** | 1.0000 |
| **True Positives (TP)** | 613 |
| **False Positives (FP)** | 0 |
| **True Negatives (TN)** | 3,335 |
| **False Negatives (FN)** | 0 |

### 2. HashingTF vs CountVectorizer Comparison

| Feature Method | Feature Dim | Train Time (s) | Accuracy | F1-Score |
| :--- | :--- | :--- | :--- | :--- |
| **CountVectorizer + IDF** | 5,000 | 2.3874s | 1.0000 | 1.0000 |
| **HashingTF + IDF** | 4,096 | 2.9903s | 1.0000 | 1.0000 |

- **CountVectorizer:** Requires a preliminary vocabulary building pass over the data, requiring memory for the dictionary, but eliminates feature collisions.
- **HashingTF:** Applies MurmurHash3 to map tokens to feature indices directly. Fast and stateless, but subject to potential hash collisions.

---

## ❓ Viva Voce Questions & Answers (25 Questions)

### Q1: What is the main objective of Practical 11?
**A:** To implement a large-scale, distributed SMS spam detection system using PySpark MLlib and Logistic Regression trained via Distributed Stochastic Gradient Descent (DSGD) / L-BFGS.

### Q2: Why is Apache Spark needed for SMS spam detection at SecureComm Inc.?
**A:** SecureComm processes over 50 million messages daily. Single-node tools (like scikit-learn) cannot fit multi-gigabyte datasets in RAM or process them efficiently. Spark distributes data partitions across cluster nodes for parallel processing.

### Q3: What is Logistic Regression and how does it perform binary classification?
**A:** Logistic Regression models the probability $P(Y=1|X)$ using the sigmoid function $\sigma(z) = \frac{1}{1 + e^{-z}}$, where $z = w^T x + b$. If $P(Y=1|X) \ge 0.5$, it predicts the positive class (spam).

### Q4: What is Distributed Stochastic Gradient Descent (DSGD)?
**A:** DSGD divides training data across multiple worker nodes. Each worker computes gradient updates on its local mini-batches. Local gradients are aggregated to update the global model weight vector $w$.

### Q5: Does PySpark MLlib `LogisticRegression` expose a user-controlled `batchSize` parameter?
**A:** No. PySpark MLlib's built-in `LogisticRegression` uses L-BFGS (Quasi-Newton) or OWL-QN optimization across RDD partitions. It does not expose a mini-batch size hyperparameter directly.

### Q6: What is the difference between Batch GD, Stochastic GD (SGD), and Mini-Batch GD?
**A:** 
- **Batch GD:** Computes gradients using the entire dataset per iteration (slow for big data).
- **Stochastic GD:** Computes gradient using 1 sample at a time (noisy updates).
- **Mini-Batch GD:** Computes gradient over small batches (e.g., 64–512 samples), balancing speed and stability.

### Q7: What is Parameter Server Architecture in distributed ML?
**A:** A Parameter Server setup consists of dedicated server nodes that store global model parameters ($w$). Worker nodes pull current weights, compute gradients on local data partitions, and push gradient updates back to the Parameter Server.

### Q8: What stages are included in our PySpark ML Pipeline?
**A:** `RegexTokenizer` $\rightarrow$ `StopWordsRemover` $\rightarrow$ `CountVectorizer` $\rightarrow$ `IDF` $\rightarrow$ `LogisticRegression`.

### Q9: Why do we use `RegexTokenizer` instead of standard `Tokenizer`?
**A:** `RegexTokenizer` uses regular expressions (such as `\W+`) to split text on non-word characters (punctuation, symbols, spaces), providing cleaner tokenization.

### Q10: What is the purpose of `StopWordsRemover`?
**A:** It eliminates common, uninformative words ('is', 'the', 'and', 'at') that occur frequently in all documents and carry little predictive signal for spam detection.

### Q11: How does TF-IDF feature extraction work?
**A:** 
- **Term Frequency (TF):** Measures how frequently a term occurs in a document.
- **Inverse Document Frequency (IDF):** Down-weights terms that appear across many documents: $\text{IDF}(t) = \log \frac{N + 1}{\text{DF}(t) + 1}$.
- **TF-IDF:** $\text{TF-IDF}(t, d) = \text{TF}(t, d) \times \text{IDF}(t)$.

### Q12: What is the difference between `CountVectorizer` and `HashingTF`?
**A:** `CountVectorizer` builds an exact vocabulary dictionary pass over data (zero hash collisions, higher memory use). `HashingTF` uses feature hashing (MurmurHash3) to map tokens directly to vector indices (stateless, zero memory for dictionary, potential hash collisions).

### Q13: What is Regularization in Logistic Regression?
**A:** Regularization adds a penalty term to the loss function to prevent overfitting by constraining coefficient magnitudes.

### Q14: What is L1 vs L2 Regularization, and what does `elasticNetParam` control?
**A:** 
- **L1 (Lasso):** Penalty = $\lambda \|w\|_1$. Drives unimportant weights to exact zero (feature selection).
- **L2 (Ridge):** Penalty = $\frac{\lambda}{2} \|w\|_2^2$. Shrinks weights smoothly.
- **`elasticNetParam` ($\alpha$):** Set to `0.0` for L2 Ridge, `1.0` for L1 Lasso, and `0.0 < alpha < 1.0` for ElasticNet combination.

### Q15: What is `regParam` in Spark MLlib Logistic Regression?
**A:** It specifies the regularization strength ($\lambda$). Higher values penalize large weights more heavily to prevent overfitting.

### Q16: Why is `maxIter` important?
**A:** `maxIter` sets the upper bound on optimization iterations. If `maxIter` is too low, the solver may stop before convergence; if too high, training takes longer without significant improvement.

### Q17: Which class is defined as the positive class in our spam detection model?
**A:** `spam` (assigned numeric index `1.0`), while `ham` is assigned index `0.0`.

### Q18: What is Precision and Recall in SMS spam detection?
**A:** 
- **Precision:** $\frac{TP}{TP + FP}$ — Of all messages flagged as spam, what percentage is actually spam?
- **Recall:** $\frac{TP}{TP + FN}$ — Of all actual spam messages, what percentage did the model catch?

### Q19: Why are False Positives particularly bad in spam detection?
**A:** A False Positive means a legitimate email/SMS (ham) was wrongly classified as spam and blocked or sent to the spam folder, causing the user to miss important messages.

### Q20: What happens when you lower the classification probability threshold from 0.50 to 0.30?
**A:** Lowering the threshold increases **Recall** (catches more spam messages), but increases **False Positives** (flags more legitimate ham messages as spam).

### Q21: What is ROC-AUC?
**A:** Area Under the Receiver Operating Characteristic curve. It plots True Positive Rate vs False Positive Rate across all thresholds. An AUC of 1.0 indicates perfect class separation.

### Q22: What does the Confusion Matrix show?
**A:** A $2 \times 2$ grid summarizing True Negatives (TN), False Positives (FP), False Negatives (FN), and True Positives (TP).

### Q23: Why do we use `randomSplit([0.8, 0.2], seed=42)`?
**A:** It partitions the dataset into 80% for model training and 20% for evaluation, using a fixed random seed for 100% reproducible results.

### Q24: How does dataset scaling affect Spark MLlib training time?
**A:** As dataset size increases, training time scales sub-linearly to linearly across distributed partitions, demonstrating cluster scalability.

### Q25: How do you save and reuse a Spark ML Pipeline Model?
**A:** Call `model.save("path/to/model")` to save the fitted pipeline artifacts, and reload using `PipelineModel.load("path/to/model")`.

---

## 📌 Summary of Output Artifacts
- [`output/evaluation_metrics.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/evaluation_metrics.csv)
- [`output/hyperparameter_results.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/hyperparameter_results.csv)
- [`output/dataset_size_comparison.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/dataset_size_comparison.csv)
- [`output/training_method_comparison.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/training_method_comparison.csv)
- [`output/tfidf_vs_hashingtf.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/tfidf_vs_hashingtf.csv)
- [`output/predictions.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/predictions.csv)
- [`output/confusion_matrix.png`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/confusion_matrix.png)
- [`output/learning_curve.png`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/learning_curve.png)
- [`output/training_time_comparison.png`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/training_time_comparison.png)
- [`output/practical11_report.txt`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-11/output/practical11_report.txt)
