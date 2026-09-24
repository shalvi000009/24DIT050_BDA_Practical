# Practical 6 - E-Commerce Sales Analysis Using Spark DataFrames

**Student ID:** 24DIT050  
**Course:** CSUE301 - Big Data Analytics  
**Practical Number:** 6  
**Topic:** E-Commerce Sales Analysis Using Spark DataFrames  

---

## 1. Problem Definition
ShopSmart Analytics Pvt. Ltd. wants to analyze e-commerce sales data to identify top-performing product categories, highest selling products by volume, revenue contribution by city, category rankings, and actionable business insights using PySpark DataFrames.

## 2. Objective
- Load e-commerce sales dataset into a PySpark DataFrame.
- Display DataFrame schema and validate data types of all columns.
- Calculate revenue for each order (`Revenue = Quantity * Price`).
- Compute category-wise total revenue and rank categories.
- Determine top-selling products based on quantity sold.
- Calculate city-wise revenue and extract top 3 revenue-generating cities.
- Calculate average revenue per order.
- Perform discount analysis (apply 10% discount to recompute revenue).
- Identify the least-performing product category based on discounted revenue.
- Handle monthly revenue summary analysis and limitations cleanly.
- Derive business insights and post-lab recommendations.

## 3. Dataset Description
The dataset `sales_data.csv` contains 15 order records with the following attributes:
- `OrderID`: Unique order identifier (String, e.g., `0001`)
- `Product`: Name of the product sold (String, e.g., `Laptop`)
- `Category`: Category classification (String, e.g., `Electronics`)
- `Quantity`: Number of units ordered (Integer)
- `Price`: Price per unit in INR (Integer)
- `City`: City location of the customer (String, e.g., `Ahmedabad`)

## 4. Implementation & Tools Used
- **Language:** Python 3.11
- **Framework:** Apache Spark / PySpark 3.5.1
- **Storage / Format:** CSV (`sales_data.csv`)
- **Execution Environment:** PySpark local SparkSession (`E-Commerce Sales Analysis`)

## 5. Analysis Performed & Code Workflow
1. **Schema & Data Types:** Defined explicit schema using `StructType` (`OrderID` as String, `Quantity` and `Price` as Integers).
2. **Revenue Calculation:** `df.withColumn("Revenue", col("Quantity") * col("Price"))`.
3. **Category-wise Revenue:** Grouped by `Category`, aggregated total revenue, sorted descending.
4. **Top-Selling Products:** Grouped by `Product`, aggregated total quantity sold, sorted descending.
5. **City-wise Revenue & Top 3:** Grouped by `City`, aggregated total revenue, limited to top 3 cities (`Ahmedabad`, `Surat`, `Rajkot`).
6. **Average Revenue per Order:** Calculated mean order revenue (`INR 38,960.00`).
7. **Discount Analysis:** Created `Discount` (`Revenue * 0.10`) and `Final_Revenue` (`Revenue - Discount`).
8. **Least-Performing Category:** Identified `Clothing` as the lowest revenue category (`INR 19,710.00` after discount).
9. **Monthly Revenue Handling:** Verified date column requirement and documented PySpark `to_date()` and `month()` transformation logic.
10. **Output Export:** Automatically generated individual output text files in `outputs/` directory.

## 6. Business Insights
- **Top Category:** Electronics is the highest revenue-generating category (`INR 385,000.00`).
- **Top Product:** TShirt has the highest quantity sold (`18 units`).
- **Top City:** Ahmedabad contributes the highest city revenue (`INR 251,400.00`), followed by Surat (`INR 176,000.00`) and Rajkot (`INR 95,000.00`).
- **Least-Performing Category:** Clothing generates the lowest revenue (`INR 21,900.00` gross, `INR 19,710.00` net after 10% discount).
- **Actionable Recommendations:** Maintain high inventory for Electronics and high-volume items (TShirts, Mobiles), while launching promotional bundles and marketing campaigns for lower-performing categories like Clothing.

## 7. Viva Q&A and Post-Lab Questions
Complete student-friendly answers for viva questions (Q1–Q5) and post-lab analysis are documented in [**`viva_answers.txt`**](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-6/viva_answers.txt).

## 8. How to Run
1. Open terminal inside the project directory:
   ```bash
   cd Practical-6
   ```
2. Execute the PySpark script:
   ```bash
   python practical6.py
   ```
3. Output text files will be updated automatically in the `outputs/` folder.
