# Practical 5 - MongoDB Installation, CRUD Operations, and Aggregation

**Student ID:** 24DIT050  
**Course:** CSUE301 - Big Data Analytics  
**Practical Number:** 5  
**Topic:** Install MongoDB and Perform CRUD Operations and Data Aggregation using MongoDB  

---

## 1. Problem Definition
Perform Create, Read, Update, and Delete (CRUD) operations using MongoDB. Students work with a NoSQL document database to understand how unstructured and semi-structured data is managed at scale in Big Data environments.

## 2. Objective
- Install and configure MongoDB server and Mongosh (MongoDB Shell) on Ubuntu Linux.
- Create a NoSQL database (`bda_db`) and collections (`sales`, `customers`, `products`).
- Insert single (`insertOne()`) and multiple (`insertMany()`) BSON documents.
- Query documents using conditional filter expressions (`find()`, `$gt`, `$eq`, `$in`, `sort()`, `limit()`).
- Update existing documents using `$set`, `$inc`, `updateOne()`, and `updateMany()`.
- Delete documents (`deleteOne()`, `deleteMany()`) and drop collections (`drop()`).
- Perform multi-stage analytics using the MongoDB Aggregation Pipeline (`$match`, `$group`, `$sort`, `$project`, `$sum`, `$avg`, `$max`, `$min`).

## 3. Dataset & NoSQL Document Schema
- **Database:** `bda_db`
- **Collection:** `sales`
- **Sample Document BSON Structure:**
  ```json
  {
    "_id": ObjectId("651a2b3c4d5e6f7a8b9c0d1e"),
    "order_id": "1001",
    "customer": "Shalvi Patel",
    "category": "Electronics",
    "product": "Laptop",
    "quantity": 2,
    "price": 55000,
    "city": "Ahmedabad",
    "status": "Completed"
  }
  ```

## 4. Implementation & Technologies
- **Operating System:** Ubuntu Linux (WSL)
- **Database Engine:** MongoDB Community Server (Version 6.x / 7.x)
- **Shell Client:** Mongosh (MongoDB Shell)
- **Data Format:** BSON (Binary JSON)
- **Architecture:** NoSQL Document-Oriented Database

## 5. Key Analysis & Workflow Executed
1. **Database & Collection Initialization:** Switched to `bda_db` and created sales collection using `use bda_db` and `db.createCollection("sales")`.
2. **Document Insertion (Create):** Inserted individual order documents using `insertOne()` and batch inserted bulk sales records using `insertMany()`.
3. **Data Querying (Read):** Executed basic queries with `find()`, filtered high-value orders using `$gt: 20000`, and sorted output descending by price.
4. **Document Updates (Update):** Updated record status using `updateOne({ order_id: "1001" }, { $set: { status: "Delivered" } })` and incremented quantities using `$inc`.
5. **Document Deletion (Delete):** Removed cancelled orders using `deleteMany({ status: "Cancelled" })`.
6. **Aggregation Pipeline Analysis:** Constructed a multi-stage aggregation pipeline to compute category-wise total revenue and average order prices:
   ```javascript
   db.sales.aggregate([
     { $match: { status: "Completed" } },
     { $group: { _id: "$category", totalRevenue: { $sum: { $multiply: ["$quantity", "$price"] } }, avgPrice: { $avg: "$price" } } },
     { $sort: { totalRevenue: -1 } }
   ]);
   ```

## 6. Key Questions & Analysis (Viva Answers)

**Q1. What is MongoDB and how does it differ from relational databases?**  
MongoDB is a NoSQL document-oriented database that stores semi-structured data as flexible BSON documents. Unlike relational SQL databases which use rigid tables, rows, columns, and fixed schemas, MongoDB uses collections, dynamic documents, and flexible schemas.

**Q2. What are CRUD operations and why are they fundamental?**  
CRUD stands for **Create, Read, Update, Delete**. They represent the four fundamental data persistence operations required to build, manage, and query any application database system.

**Q3. How does MongoDB store data (BSON format)?**  
MongoDB stores documents internally in **BSON (Binary JSON)** format. BSON extends JSON by providing binary serialization and supporting additional data types such as `ObjectId`, `Date`, 64-bit Integer, `Binary data`, and Regex.

**Q4. What is the aggregation pipeline in MongoDB?**  
The aggregation pipeline is a framework for data processing and analysis where documents pass through a sequence of multi-stage operators (e.g., `$match`, `$group`, `$sort`, `$project`, `$limit`). Each stage transforms the document stream to compute aggregates like sums, averages, min/max values, and counts.

**Q5. When should NoSQL databases be preferred over SQL databases?**  
NoSQL databases should be preferred when handling massive volumes of unstructured or semi-structured data, rapidly evolving schemas, real-time analytics, horizontal auto-sharding/scaling across clusters, and high write throughput requirements.

## 7. How to Run / Commands Executed

```javascript
// 1. Start Mongosh Shell and Connect
mongosh

// 2. Select Database and Create Collection
use bda_db
db.createCollection("sales")

// 3. Insert Documents (Create)
db.sales.insertOne({
  order_id: "1001",
  customer: "Shalvi Patel",
  category: "Electronics",
  product: "Laptop",
  quantity: 2,
  price: 55000,
  city: "Ahmedabad",
  status: "Completed"
});

db.sales.insertMany([
  { order_id: "1002", category: "Electronics", product: "Mobile", quantity: 5, price: 25000, city: "Surat", status: "Completed" },
  { order_id: "1003", category: "Clothing", product: "TShirt", quantity: 8, price: 800, city: "Ahmedabad", status: "Completed" }
]);

// 4. Query Documents (Read)
db.sales.find()
db.sales.find({ category: "Electronics" }).pretty()
db.sales.find({ price: { $gt: 20000 } })

// 5. Update Documents (Update)
db.sales.updateOne(
  { order_id: "1001" },
  { $set: { status: "Delivered" } }
);

// 6. Delete Documents (Delete)
db.sales.deleteOne({ order_id: "1003" });

// 7. Aggregation Pipeline Analysis
db.sales.aggregate([
  { $match: { status: "Completed" } },
  { $group: {
      _id: "$category",
      totalRevenue: { $sum: { $multiply: ["$quantity", "$price"] } },
      avgPrice: { $avg: "$price" },
      totalOrders: { $sum: 1 }
    }
  },
  { $sort: { totalRevenue: -1 } }
]);
```
