# BDA Practical 12: Customer Sales Dashboard Using Interactive Analytics

**Student ID:** 24DIT050  
**Course:** CSUE301 - Big Data Analytics  
**Scenario:** Retail Analytics Executive Dashboard  
**Frameworks:** Streamlit, Plotly Express, Pandas, NumPy, Python 3.11+  

---

## 📌 Problem Definition & Objectives
Retail executives require real-time visibility into sales revenue, profit margins, regional performance, product line profitability, and customer segment trends. Static tabular reports fail to provide the interactive exploratory features needed to answer ad-hoc management questions.

This practical constructs a complete, interactive **Customer Sales Dashboard** using **Streamlit** and **Plotly**. The application ingests a multi-year Superstore Sales dataset, performs automated data quality validation, calculates multi-dimensional business KPIs, and provides responsive cross-filtering across regions, product categories, customer segments, and date ranges.

---

## 🛠️ Technology Stack
- **Language:** Python 3.11+
- **Interactive Web Dashboard:** Streamlit (`streamlit>=1.25.0`)
- **Data Visualization & Plotting:** Plotly Express & Graph Objects (`plotly>=5.10.0`)
- **Data Wrangling & Analytics:** Pandas & NumPy
- **Environment:** Local Python Virtual Environment (`.venv`)

---

## 📁 Project Structure

```
Practical-12/
├── data/
│   └── superstore_sales.csv            # Multi-year Superstore Sales dataset (5,000 records)
├── output/
│   ├── cleaned_sales.csv               # Validated, cleaned, and feature-engineered dataset
│   ├── data_quality_report.txt         # Data quality audit report (missing values, duplicates, losses)
│   ├── sales_summary.csv               # Overall executive KPI summary CSV
│   ├── category_summary.csv            # Sub-category sales, profit, orders, and margin breakdown
│   ├── region_summary.csv              # Region & state performance summary CSV
│   ├── quarterly_summary.csv           # Year & quarter revenue and margin comparison CSV
│   └── practical12_report.txt          # Comprehensive technical and analytical report
├── app.py                              # Interactive Streamlit + Plotly web dashboard app
├── data_processing.py                  # Data cleaning, validation, and summary export engine
├── generate_dataset.py                 # Multi-year Superstore Sales dataset generator
├── requirements.txt                    # Project dependencies
└── README.md                           # Beginner-friendly documentation & 25 Viva Q&A
```

---

## 📊 Dataset Schema

| Field Name | Data Type | Description | Sample Value |
| :--- | :--- | :--- | :--- |
| `Order ID` | String | Unique Transaction Identifier | `CA-2023-145892` |
| `Order Date` | Date (YYYY-MM-DD) | Date order was placed | `2023-05-15` |
| `Ship Date` | Date (YYYY-MM-DD) | Date order was shipped | `2023-05-18` |
| `Customer ID` | String | Unique Customer Identifier | `CG-12520` |
| `Customer Name` | String | Full Name of Customer | `Claire Gute` |
| `Segment` | String | Customer Market Segment | `Consumer`, `Corporate`, `Home Office` |
| `Region` | String | Geographic Region | `East`, `West`, `Central`, `South` |
| `State` | String | US State | `California`, `Texas`, `New York` |
| `City` | String | US City | `Los Angeles`, `Chicago`, `Miami` |
| `Category` | String | Top-level Product Category | `Furniture`, `Office Supplies`, `Technology` |
| `Sub-Category` | String | Specific Product Group | `Chairs`, `Tables`, `Phones`, `Binders` |
| `Product Name` | String | Item Name | `Hon Executive Leather Chair` |
| `Sales` | Float | Gross Revenue ($) | `1108.32` |
| `Quantity` | Integer | Units Purchased | `4` |
| `Discount` | Float | Promotional Discount % | `0.15` |
| `Profit` | Float | Net Earnings / Loss ($) | `217.44` |

---

## 🚀 Quick Start & Execution

### 1. Setup Virtual Environment

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

### 3. Generate Superstore Dataset & Process Data
```bash
python generate_dataset.py --rows 5000 --output data/superstore_sales.csv
python data_processing.py
```

### 4. Launch Interactive Web Dashboard
```bash
streamlit run app.py
```
*(The dashboard will automatically open in your default browser at `http://localhost:8501`)*

---

## 📐 KPI Definitions & Formulas

1. **Total Sales ($):** $\sum \text{Sales}$
2. **Total Profit ($):** $\sum \text{Profit}$
3. **Distinct Orders (#):** $\text{CountDistinct}(\text{Order ID})$
4. **Total Customers (#):** $\text{CountDistinct}(\text{Customer ID})$
5. **Total Units Sold (#):** $\sum \text{Quantity}$
6. **Average Order Value (AOV):** $\frac{\text{Total Sales}}{\text{Distinct Orders}}$
7. **Profit Margin (%):** $\left( \frac{\text{Total Profit}}{\text{Total Sales}} \right) \times 100$

---

## 📈 Measured Empirical Results (5,000 Records)

- **Total Sales:** **$13,641,279.68**
- **Total Profit:** **$2,122,428.16**
- **Distinct Orders:** **4,997**
- **Distinct Customers:** **20**
- **Total Items Sold:** **27,817 units**
- **Average Order Value:** **$2,729.89**
- **Overall Profit Margin:** **15.56%**

### Category & Regional Findings
- **Top Sub-Category by Sales:** `Bookcases` ($1,304,131.19, 4.77% Margin)
- **Top Sub-Category by Net Profit:** `Chairs` ($217,448.40, 19.62% Margin)
- **Loss-Leader Sub-Category:** `Tables` generated $1.14M in sales but yielded only $12.9K in profit (1.14% Margin) due to discounts > 20%.
- **Top State by Revenue & Profit:** `California` ($1.71M Sales, $290.4K Profit).

---

## ❓ Viva Voce Questions & Answers (25 Questions)

### Q1: What is the main objective of Practical 12?
**A:** To design, build, and deploy an interactive Customer Sales Analytics Dashboard that allows management to monitor KPIs, explore sales across dimensions, and make data-driven decisions.

### Q2: Why are interactive dashboards superior to static tabular reports for executive decision-making?
**A:** Interactive dashboards enable dynamic cross-filtering, real-time drill-down, visual trend detection, and immediate answers to ad-hoc business questions without writing new queries.

### Q3: What technology stack was selected for this implementation and why?
**A:** Python with Streamlit, Plotly, and Pandas was chosen because it runs 100% locally without paid software licenses (like Tableau/Power BI Desktop), provides responsive web UI controls, and allows full programmatic data validation.

### Q4: How is "Total Orders" calculated correctly in sales analytics?
**A:** By taking the count of distinct `Order ID` values (`COUNT_DISTINCT`), not simply counting the total number of rows (which represents item line entries).

### Q5: What is Average Order Value (AOV) and why is it useful?
**A:** $\text{AOV} = \frac{\text{Total Sales}}{\text{Distinct Orders}}$. It measures the average revenue generated per customer transaction, helping evaluate cross-selling and upselling strategies.

### Q6: How is Profit Margin calculated?
**A:** $\text{Profit Margin (\%)} = \left( \frac{\text{Total Profit}}{\text{Total Sales}} \right) \times 100$.

### Q7: Why shouldn't negative-profit rows be deleted during data cleaning?
**A:** Negative profit transactions represent legitimate business transactions resulting from promotional discounts, returns, or loss-leader pricing. Deleting them would falsely inflate calculated profitability.

### Q8: What data quality checks were performed on the Superstore dataset?
**A:** 1) Missing values check across all columns, 2) Duplicate row detection, 3) Date parsing validation, 4) Non-positive sales/quantity checks, 5) Discount range validation (0.0 to 1.0).

### Q9: What is a "Loss-Leader" product in retail analytics?
**A:** A product sold at a low or negative profit margin to attract customers, with the expectation that customers will purchase other high-margin items.

### Q10: Which product sub-category generated high revenue but very low profit in our dataset?
**A:** `Tables` generated $1.14M in sales but yielded a profit margin of only 1.14% ($12,949) due to heavy promotional discounting.

### Q11: Which product category exhibited the highest profit margin?
**A:** `Technology` sub-categories (Accessories, Phones, Copiers) exhibited consistently high margins between 17% and 19.8%.

### Q12: Which US state achieved the highest sales volume and net profit?
**A:** `California` ($1.71M Sales, $290.4K Profit).

### Q13: How does the sidebar filter interaction work in Streamlit?
**A:** Selecting a filter option triggers a reactive re-execution of `app.py`. Pandas filters the underlying DataFrame, and Plotly updates all charts and metric cards in real-time.

### Q14: What is the purpose of the `st.cache_data` decorator in Streamlit?
**A:** It caches the loaded and processed DataFrame in memory so that subsequent filter interactions do not re-read or re-parse the CSV file from disk, ensuring fast rendering.

### Q15: How does the monthly revenue trend chart help business managers?
**A:** It visualizes seasonality, growth trajectories, and spikes or dips in monthly revenue and net profit over time.

### Q16: What is a drill-down feature in interactive analytics?
**A:** The ability to navigate from high-level summary metrics (e.g. Region) down to granular levels (State $\rightarrow$ City $\rightarrow$ Individual Transactions).

### Q17: How is Customer Segmentation analyzed in the dashboard?
**A:** Transactions are grouped by market segments (`Consumer`, `Corporate`, `Home Office`), evaluating Total Sales, Net Profit, Order Count, and AOV per segment.

### Q18: What is a high-value customer in customer analytics?
**A:** A customer with high lifetime spend, frequent distinct order counts, and a high Average Order Value (AOV).

### Q19: Why is dual-axis visualization useful for Sales vs Profit?
**A:** It allows simultaneous comparison of gross revenue (large scale) and net profit (smaller scale) on the same timeline or category axis.

### Q20: How do discounts impact gross sales vs net profit?
**A:** High discounts increase gross sales volume but reduce unit revenue, often causing net profit to turn negative if the discount exceeds the gross product margin.

### Q21: What is the difference between `Category` and `Sub-Category` in the Superstore dataset?
**A:** `Category` is a high-level grouping (e.g., Furniture), while `Sub-Category` represents specific product classes (e.g., Chairs, Tables, Bookcases).

### Q22: What export options are provided in the transaction explorer tab?
**A:** Users can search transactions using a keyword search bar and download the filtered transaction subset as a CSV file via `st.download_button`.

### Q23: What recommendation would you give management regarding discounting on Tables?
**A:** Restrict promotional discounts on Tables to a maximum of 15% to eliminate negative-profit transactions and restore category margins.

### Q24: How can the dashboard be deployed for enterprise web access?
**A:** By deploying the Streamlit application container to Streamlit Community Cloud, AWS EC2, Docker, or an internal corporate server.

### Q25: What is the role of `data_processing.py` versus `app.py`?
**A:** `data_processing.py` handles offline data cleaning, validation reporting, and CSV summary generation, while `app.py` delivers the live interactive web user interface.

---

## 📌 Summary of Output Artifacts
- [`output/cleaned_sales.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-12/output/cleaned_sales.csv)
- [`output/data_quality_report.txt`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-12/output/data_quality_report.txt)
- [`output/sales_summary.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-12/output/sales_summary.csv)
- [`output/category_summary.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-12/output/category_summary.csv)
- [`output/region_summary.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-12/output/region_summary.csv)
- [`output/quarterly_summary.csv`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-12/output/quarterly_summary.csv)
- [`output/practical12_report.txt`](file:///c:/Users/hp/OneDrive/Desktop/.vscode/BDA/Practical-12/output/practical12_report.txt)
