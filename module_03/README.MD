# Data Engineering Zoomcamp 2026 – Homework 3

Homework answers and SQL queries for the BigQuery module of the DE Zoomcamp 2026.

---

## BigQuery Setup

Create an external table using the Yellow Taxi Trip Records.

```sql
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.yellow_tripdata_parquet_ext`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://newyork-taxi/yellow_tripdata_*.parquet']
);
```

Create a regular, materialized table in BigQuery using the Yellow Taxi Trip Records. Do not partition or cluster this table.

```sql
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_parquet
AS
SELECT * FROM zoomcamp.yellow_tripdata_parquet_ext;
```

---

## Question 1 — Counting Records

**What is the count of records for the 2024 Yellow Taxi Data?**

```sql
SELECT COUNT(*) FROM zoomcamp.yellow_tripdata_parquet;
```

**Answer:** 20,332,093

---

## Question 2 — Data Read Estimation

**Write a query to count the distinct number of PULocationIDs for the entire dataset on both tables. What is the estimated amount of data that will be read?**

```sql
-- External Table
SELECT DISTINCT PULocationID FROM zoomcamp.yellow_tripdata_parquet_ext;

-- Materialized Table
SELECT DISTINCT PULocationID FROM zoomcamp.yellow_tripdata_parquet;
```

**Answer:** 0 MB for the External Table and 155.12 MB for the Materialized Table

---

## Question 3 — Understanding Columnar Storage

**Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?**

```sql
-- Single column
SELECT PULocationID FROM zoomcamp.yellow_tripdata_parquet;

-- Two columns
SELECT PULocationID, DOLocationID FROM zoomcamp.yellow_tripdata_parquet;
```

**Answer:** BigQuery is a columnar database and only scans the specific columns requested in the query. Querying two columns (`PULocationID`, `DOLocationID`) requires reading more data than querying one column (`PULocationID`), leading to a higher estimated number of bytes processed.

---

## Question 4 — Counting Zero Fare Trips

**How many records have a `fare_amount` of 0?**

```sql
SELECT COUNT(*) FROM zoomcamp.yellow_tripdata_parquet
WHERE fare_amount = 0;
```

**Answer:** 8,333

---

## Question 5 — Partitioning and Clustering

**What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)**

```sql
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_parquet_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp.yellow_tripdata_parquet;
```

**Answer:** Partition by `tpep_dropoff_datetime` and Cluster on `VendorID`

---

## Question 6 — Partition Benefits

**Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive).**
**Use the materialized table first, note the estimated bytes, then swap to the partitioned table from question 5. What are these values?**

```sql
-- Materialized table
SELECT DISTINCT VendorID
FROM zoomcamp.yellow_tripdata_parquet
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'

-- Partitioned and clustered table
SELECT DISTINCT VendorID
FROM zoomcamp.yellow_tripdata_parquet_partitioned_clustered
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
```

**Answer:** 310.24 MB for the non-partitioned table and 26.84 MB for the partitioned table

---

## Question 7 — External Table Storage

**Where is the data stored in the External Table?**

**Answer:** GCP Bucket

---

## Question 8 — Clustering Best Practices

**Is it best practice in BigQuery to always cluster your data?**

**Answer:** True

---

## Question 9 — Understanding Table Scans (No Points)

**Write a `SELECT count(*)` query from the materialized table. How many bytes does it estimate will be read? Why?**

```sql
SELECT COUNT(*)
FROM zoomcamp.yellow_tripdata_parquet;
```

**Answer:** BigQuery estimates 0 bytes for `COUNT(*)` on a materialized table because the row count is stored in the table metadata and does not require scanning any columns.

---

## Submission

Submit your solutions here: [DE Zoomcamp 2026 — Homework 3](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3)