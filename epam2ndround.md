### 1. SQL — Very important

Expect questions such as:

* Find the **2nd/3rd/Nth highest salary**.
* Difference between `RANK()`, `DENSE_RANK()` and `ROW_NUMBER()`.
* Find the **top 3 employees by salary in each department**.
* Find employees whose salary is greater than their department average.
* Find duplicate records and remove duplicates.
* Find employees who joined in the **last N months**.
* `LEAD()` vs `LAG()`.
* `FIRST_VALUE()` vs `ROW_NUMBER()`.
* CTE vs subquery vs temporary table.
* `WHERE` vs `HAVING`.
* `UNION` vs `UNION ALL`.
* Inner/Left/Full joins and situations where each is useful.
* What happens when joining tables containing `NULL`s?
* How would you optimize a slow SQL query?
* What are indexes and when do they help?
* Explain normalization: **1NF, 2NF, 3NF**.
* Write SQL to calculate running totals.
* Find consecutive days of activity.
* Identify gaps in transaction data.

**Likely senior-level scenario:**

> You have a 5-billion-row transaction table. A query that previously took 5 minutes now takes 45 minutes. How would you troubleshoot and optimize it?

You should be ready to discuss **execution plan, partitioning, indexes where applicable, data volume, joins, predicate pushdown, statistics, skew, unnecessary scans, and aggregation strategy**.

---

## 2. PySpark / Spark — Very important

This is probably one of the biggest areas for your interview.

### Fundamentals

Know:

* What is Spark?
* Driver vs Executor.
* Cluster manager.
* SparkSession.
* RDD vs DataFrame vs Dataset.
* Transformations vs Actions.
* Narrow vs Wide transformations.
* Lazy evaluation.
* DAG.
* Stages and Tasks.
* Shuffle.

A very common question:

> **Can you write an action in between Spark transformations?**

For example:

```python
df = spark.read.parquet(path)

df = df.filter("salary > 50000")

df.count()

df = df.groupBy("department").count()

df.show()
```

Yes. `count()` is an **action**, and Spark will execute the transformations required to produce the result at that point. Later transformations create another execution path when another action is triggered.

---

### Spark optimization

Be very comfortable with:

* `repartition()` vs `coalesce()`
* Broadcast joins
* Partitioning
* Bucketing
* Caching/persisting
* Predicate pushdown
* Column pruning
* Shuffle
* Data skew
* Salting
* Adaptive Query Execution (AQE)
* Small-file problem
* Serialization
* Spark UI

Expect something like:

> You have a 1 TB fact table and a 10 MB dimension table. How would you join them efficiently?

Expected direction:

```python
from pyspark.sql.functions import broadcast

result = fact_df.join(
    broadcast(dim_df),
    "customer_id"
)
```

Then they may ask:

> What if the dimension table is 5 GB?

Now you should discuss why blindly broadcasting it is dangerous and consider **shuffle join, partitioning, AQE, etc.**

---

# 3. Databricks / Delta Lake

Since the position is senior-level, don't stop at "Databricks is a cloud Spark platform."

Know:

### Delta Lake

* ACID transactions
* Schema enforcement
* Schema evolution
* Time travel
* `MERGE`
* `OPTIMIZE`
* `VACUUM`
* Z-Ordering
* Change Data Feed
* Partitioning
* Delta transaction log

Example scenario:

> An RDBMS sends 10 million new/updated records every day. How would you incrementally load them into Databricks?

You should be able to explain:

**Source → ingestion → Bronze → Silver → Gold**

and discuss:

* watermark / timestamp
* CDC
* incremental extraction
* checkpointing
* Delta `MERGE`
* handling updates/deletes
* idempotency
* retries
* audit/control tables

For example:

```sql
MERGE INTO target t
USING source s
ON t.id = s.id

WHEN MATCHED THEN
  UPDATE SET *

WHEN NOT MATCHED THEN
  INSERT *
```

Then expect:

> What happens if the pipeline fails halfway?

This is where they are testing **idempotency and fault tolerance**, not just syntax.

---

# 4. AWS

For a Senior Data Engineer role, prepare:

### S3

* Object storage
* Partitioning
* File formats
* Parquet vs CSV
* Small files
* Lifecycle policies
* Encryption
* IAM

### Glue

* Glue Data Catalog
* Crawlers
* ETL jobs
* DynamicFrames vs DataFrames
* Glue workflows

### AWS data architecture

You could get:

> Design a pipeline to ingest data from an on-premise Oracle database into AWS and finally expose business-ready data for reporting.

Be prepared to explain something like:

**Oracle → ingestion → S3 → Glue/Spark/Databricks → Delta/Parquet → curated layer → BI**

and discuss:

* incremental loading
* CDC
* retries
* monitoring
* security
* partitioning
* schema changes

---

# 5. Python

Don't prepare only basic Python.

For senior level, expect:

### Core Python

* List vs tuple vs set vs dictionary
* Mutable vs immutable
* Shallow vs deep copy
* `*args`, `**kwargs`
* Lambda
* List/dict comprehensions
* Iterators
* Generators
* Decorators
* Exception handling
* Context managers
* `map`, `filter`, `reduce`
* `zip`
* `enumerate`

### Coding

You may get problems such as:

```text
Find duplicate elements
Find first non-repeating character
Reverse a string
Two Sum
Merge two sorted arrays
Find missing number
Find frequency of elements
Flatten a nested list
Find top K elements
```

But for senior roles, they may ask:

> Given a 20 GB file, how would you process it using Python without loading everything into memory?

You should immediately think:

**generators / streaming / chunk processing**

rather than:

```python
data = open(file).read()
```

---

# 6. Data Engineering scenarios — VERY important

This is where Round 2 can become significantly different from Round 1.

Expect questions like:

### Scenario 1 — Incremental load

> Source is Oracle. Target is Databricks. Source contains 1 billion records. How do you load only changed records?

Discuss:

* timestamp column
* CDC
* SCN/change tracking if applicable
* watermark
* control table
* checkpoint
* late-arriving data
* retries
* idempotency

---

### Scenario 2 — Duplicate data

> Your pipeline ran twice and inserted duplicate records. How do you prevent this?

Talk about:

* idempotency
* business key
* deduplication
* Delta `MERGE`
* batch/run ID
* checkpointing

---

### Scenario 3 — Data skew

> One customer has 500 million transactions while other customers have only a few thousand. Your Spark job is extremely slow. Why?

Answer:

**Data skew → one/few partitions become huge → one task takes much longer → stage waits for straggler task.**

Then discuss:

* salting
* AQE skew join handling
* repartitioning
* broadcast where appropriate

---

### Scenario 4 — Small files

> Your Databricks table contains millions of tiny files and performance is degrading. What would you do?

Discuss:

* compaction
* `OPTIMIZE`
* appropriate partition strategy
* avoiding excessive partitions
* file size
* ingestion design

---

# 7. Data pipeline design

They may give you a complete requirement:

> Design a scalable data pipeline for customer transactions.

You should be able to draw/explain:

```text
             Source Systems
            /      |       \
        Oracle    APIs     Files
           \       |       /
            Ingestion Layer
                  |
                  v
             S3 / ADLS
                  |
                  v
             Bronze Layer
                  |
                  v
             Silver Layer
                  |
                  v
              Gold Layer
                  |
                  v
             BI / Analytics
```

Then explain:

* Why Bronze/Silver/Gold?
* How do you handle failures?
* How do you monitor jobs?
* How do you handle schema changes?
* How do you handle duplicates?
* How do you handle late-arriving records?
* How do you replay failed data?
* How do you secure the data?
* How do you optimize costs?

---

# 8. Databricks + SQL + Spark combined scenario

I would **especially prepare this**.

They might give you:

> You have an Orders table containing 5 billion records. Every day you receive 20 million new/updated records. Build a pipeline to process the data and produce daily customer sales.

You should be able to explain:

```text
RDBMS
  ↓
Incremental Extraction
  ↓
Bronze Delta
  ↓
Deduplication
  ↓
MERGE / CDC
  ↓
Silver Delta
  ↓
Aggregations
  ↓
Gold Delta
  ↓
Reporting
```

Then explain how Spark handles:

**partitioning → shuffle → joins → aggregation → output**

This type of question can test almost everything at once.

---

# 9. Production/support questions

Because you're interviewing for **Senior Software Data Engineer**, expect behavioral + production questions too:

* Tell me about a production issue you handled.
* A Spark job suddenly becomes 3x slower. What do you check?
* Pipeline failed at 2 AM. What would you do?
* How do you monitor data pipelines?
* How do you validate data quality?
* How do you handle bad records?
* How do you implement retry mechanisms?
* How do you make a pipeline fault tolerant?
* How do you implement logging?
* How do you handle schema changes?
* How do you handle a downstream system being unavailable?
* How do you prevent duplicate processing?

A strong senior answer should follow:

**Detect → Diagnose → Recover → Prevent recurrence**

---

# 10. Most likely "deep dive" questions

I'd make sure you can answer these without hesitation:

1. **Explain Spark architecture.**
2. **What happens internally when you call `df.groupBy().count()`?**
3. **What is a shuffle and why is it expensive?**
4. **How do you handle data skew?**
5. **Broadcast join vs shuffle join?**
6. **`repartition()` vs `coalesce()`?**
7. **What is lazy evaluation?**
8. **Transformation vs action?**
9. **What happens when an executor fails?**
10. **How does Spark achieve fault tolerance?**
11. **How do you optimize a slow Spark job?**
12. **Explain Delta Lake transaction log.**
13. **How does Delta `MERGE` work?**
14. **How do you implement incremental loading?**
15. **How do you handle duplicates?**
16. **How do you handle late-arriving data?**
17. **How do you design an idempotent pipeline?**
18. **How do you handle schema evolution?**
19. **How would you process 1 TB of data efficiently?**
20. **Design an end-to-end data pipeline.**

---

## 🔥 What I would prioritize for YOUR Round 2

Given the topics you've been practicing recently, I would spend your remaining preparation time roughly like this:

| Area                           | Priority |
| ------------------------------ | -------- |
| **PySpark / Spark internals**  | ⭐⭐⭐⭐⭐    |
| **SQL + Window Functions**     | ⭐⭐⭐⭐⭐    |
| **Databricks + Delta Lake**    | ⭐⭐⭐⭐⭐    |
| **Data Engineering scenarios** | ⭐⭐⭐⭐⭐    |
| **AWS**                        | ⭐⭐⭐⭐     |
| **Python**                     | ⭐⭐⭐⭐     |
| Hadoop/Hive/Sqoop              | ⭐⭐⭐      |
| Basic programming theory       | ⭐⭐⭐      |

**One important point:** don't prepare Round 2 as a list of definitions. At senior level, they are much more likely to ask **"Why?", "What happens internally?", "What would you do if...?", and "How would you design..."**

If you want, I can conduct a **realistic EPAM Round-2 mock interview** with you: I'll act as the interviewer and ask **one question at a time**, starting with SQL/Spark and progressively increasing the difficulty.
