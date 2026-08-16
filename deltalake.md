## Delta Tables — PySpark Explanation

A **Delta table** is a data table built on top of **Delta Lake**. It stores data as files (commonly Parquet) and maintains a **transaction log** that records changes to the table.

Think of it as:

```text
Traditional Parquet
        ↓
   Parquet files
        ↓
No transaction history
No ACID transactions
Difficult updates/deletes
```

Whereas:

```text
Delta Table
        ↓
Parquet data files
        +
Delta transaction log (_delta_log)
        ↓
ACID transactions
UPDATE / DELETE / MERGE
Time Travel
Schema enforcement
Schema evolution
```

### 1. What does a Delta table actually contain?

A Delta table typically looks like:

```text
customer_delta/
│
├── part-00000-....parquet
├── part-00001-....parquet
├── part-00002-....parquet
│
└── _delta_log/
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    ├── 00000000000000000002.json
    └── ...
```

The important part is:

```text
_delta_log
```

This is the **transaction log**.

It keeps track of which files belong to each version of the table and what operations happened.

---

# 2. Why do we need Delta if we already have Parquet?

Suppose you have:

```text
customer.parquet
```

and you want to update:

```text
customer_id = 101
```

Parquet is primarily an immutable file format. Updating a record generally means rewriting files.

Delta Lake adds a transaction layer around the files.

So you can do:

```python
delta_table.update(
    condition="customer_id = 101",
    set={"status": "'inactive'"}
)
```

Delta manages the underlying file changes and records the transaction in `_delta_log`.

---

# 3. Creating a Delta table in PySpark

```python
df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/data/customers")
```

Read it:

```python
df = spark.read \
    .format("delta") \
    .load("/data/customers")
```

You can also register it as a table:

```python
df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("customers")
```

Then:

```python
spark.table("customers").show()
```

---

# 4. INSERT

```python
new_df.write \
    .format("delta") \
    .mode("append") \
    .save("/data/customers")
```

This adds new records.

---

# 5. UPDATE

One of the major advantages of Delta is that you can perform updates.

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(
    spark,
    "/data/customers"
)

delta_table.update(
    condition="customer_id = 101",
    set={
        "status": "'inactive'"
    }
)
```

---

# 6. DELETE

```python
delta_table.delete(
    condition="customer_id = 101"
)
```

For example:

```text
Before

101  Mohan  active
102  Rahul  active
103  John   inactive
```

After:

```python
delta_table.delete("customer_id = 101")
```

```text
102  Rahul  active
103  John   inactive
```

---

# 7. MERGE — ⭐ Extremely important for interviews

`MERGE` is one of the most important Delta concepts for a **Data Engineer/PySpark interview**.

Suppose your target contains:

```text
customer_id    name      amount
-----------    --------  ------
101            Mohan     1000
102            Rahul     2000
103            John      3000
```

Incoming data:

```text
customer_id    name      amount
-----------    --------  ------
101            Mohan     1500
104            Ravi      4000
```

We want:

```text
101 → UPDATE
104 → INSERT
```

Code:

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(
    spark,
    "/data/customers"
)

(
    target.alias("target")
    .merge(
        source_df.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

Result:

```text
101  Mohan  1500   ← updated
102  Rahul  2000
103  John   3000
104  Ravi   4000   ← inserted
```

### Interview question

**Why is MERGE important?**

Because it is commonly used for:

```text
CDC
Incremental loads
Upserts
SCD Type 1
Data synchronization
```

---

# 8. ACID Transactions

Delta provides **ACID** properties.

### A — Atomicity

A transaction either succeeds completely or doesn't become visible as a partial transaction.

### C — Consistency

Transactions preserve the table's defined consistency rules.

### I — Isolation

Concurrent operations are coordinated through Delta's transaction mechanism.

### D — Durability

Committed changes remain persisted.

This is one of the biggest differences between a simple collection of Parquet files and a transactional Delta table.

---

# 9. Time Travel — ⭐ Very important

Delta maintains table versions.

For example:

```text
Version 0
   ↓
Version 1
   ↓
Version 2
   ↓
Version 3
```

You can query an older version.

```python
df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 2)
    .load("/data/customers")
)
```

You can also use a timestamp:

```python
df = (
    spark.read
    .format("delta")
    .option(
        "timestampAsOf",
        "2026-08-10 10:00:00"
    )
    .load("/data/customers")
)
```

### Why is this useful?

Suppose somebody accidentally deletes:

```text
10 million records
```

You can inspect an earlier table version.

---

# 10. Schema Enforcement

Suppose the existing Delta table has:

```text
customer_id → integer
amount      → double
```

and somebody tries to write:

```text
amount → string
```

Delta can reject incompatible schema changes instead of silently accepting them.

This protects data quality.

---

# 11. Schema Evolution

Sometimes you intentionally want to add columns.

For example:

Existing:

```text
customer_id
name
amount
```

New DataFrame:

```text
customer_id
name
amount
country
```

You can enable schema evolution during an appropriate write:

```python
(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save("/data/customers")
)
```

Now `country` can be added to the table schema.

### Important interview distinction

**Schema enforcement**:

> Prevents incompatible schema from being written.

**Schema evolution**:

> Allows supported schema changes, such as adding new columns, when explicitly enabled/configured.

---

# 12. Delta + Medallion Architecture

This is particularly relevant to Data Engineering interviews.

A typical architecture is:

```text
             Source
               ↓
          ┌──────────┐
          │  Bronze  │
          │  Delta   │
          └────┬─────┘
               ↓
          ┌──────────┐
          │  Silver  │
          │  Delta   │
          └────┬─────┘
               ↓
          ┌──────────┐
          │   Gold   │
          │  Delta   │
          └──────────┘
```

### Bronze

Raw data:

```text
Salesforce
   ↓
S3
   ↓
Bronze Delta
```

### Silver

Cleaned/transformed data:

```text
Bronze
 ↓
deduplication
 ↓
data quality
 ↓
business transformations
 ↓
Silver Delta
```

### Gold

Business-ready data:

```text
Silver
 ↓
aggregations
 ↓
business logic
 ↓
Gold Delta
```

---

# 13. Delta vs Parquet

| Feature            | Parquet | Delta |
| ------------------ | ------- | ----- |
| Columnar storage   | ✅       | ✅     |
| ACID transactions  | ❌       | ✅     |
| UPDATE             | Limited | ✅     |
| DELETE             | Limited | ✅     |
| MERGE              | ❌       | ✅     |
| Time travel        | ❌       | ✅     |
| Transaction log    | ❌       | ✅     |
| Schema enforcement | Limited | ✅     |
| Schema evolution   | Limited | ✅     |
| Built on Parquet   | —       | ✅     |

The important point is:

> **Delta doesn't replace Parquet as the underlying storage format. Delta adds a transaction/logging layer around the data files.**

---

# 14. Delta Table Architecture

Think of it like this:

```text
                 DELTA TABLE
                     │
          ┌──────────┴──────────┐
          │                     │
    Data files              Transaction log
          │                     │
      Parquet              _delta_log
          │                     │
          │              versions/actions
          │                     │
          └──────────┬──────────┘
                     ↓
              Current table state
```

This is a very important mental model.

---

# 15. What happens during UPDATE?

Suppose:

```text
part-001.parquet
```

contains:

```text
101 Mohan 1000
102 Rahul 2000
```

You execute:

```python
UPDATE customer_id = 101
SET amount = 1500
```

Delta generally doesn't modify the Parquet row in place.

Conceptually:

```text
Old file
   ↓
Remove old file from current table state
   ↓
Write new file
   ↓
Add new file to table state
   ↓
Write transaction to _delta_log
```

So Delta provides the **logical transactional view** even though the underlying files are immutable.

---

# 16. VACUUM

After updates/deletes, old files may no longer be part of the current table state.

`VACUUM` removes old unused files after the retention period.

Conceptually:

```text
Version 1
   ↓
Version 2
   ↓
Version 3

Old files
   ↓
VACUUM
   ↓
Physical cleanup
```

### Important

Don't confuse:

```text
VACUUM
```

with:

```text
Time Travel
```

Time travel depends on historical data/files still being available.

If you physically remove old files with `VACUUM`, sufficiently old versions may no longer be queryable.

---

# 17. OPTIMIZE

In Delta environments that support it, `OPTIMIZE` is used to improve file layout by compacting small files.

Suppose you have:

```text
100,000 tiny files
```

This can cause:

```text
Too many file opens
Metadata overhead
Many tiny tasks
Poor performance
```

Compaction can turn them into fewer larger files.

Conceptually:

```text
1000 tiny files
      ↓
   OPTIMIZE
      ↓
10 larger files
```

---

# 18. Z-Ordering

In environments supporting Delta Z-Ordering, frequently filtered columns can be organized to improve data skipping.

For example:

```python
OPTIMIZE customers
ZORDER BY (customer_id)
```

If queries frequently use:

```sql
WHERE customer_id = 101
```

better file organization can reduce the amount of data that needs to be read.

### Don't confuse:

```text
Partitioning
```

with:

```text
Z-Ordering
```

Partitioning creates directory-level partitions, while Z-Ordering improves locality within files.

---

# 19. Delta and CDC

Delta is very useful for **Change Data Capture** pipelines.

Suppose Salesforce sends:

```text
INSERT
UPDATE
DELETE
```

You can process the changes and use:

```python
MERGE
```

against the target Delta table.

Conceptually:

```text
Salesforce CDC
       ↓
    Bronze
       ↓
Transform
       ↓
    Silver
       ↓
     MERGE
       ↓
Target Delta
```

---

# 20. SCD Type 1 with Delta

Suppose:

```text
Customer
101 | Mohan | Hyderabad
```

New data:

```text
101 | Mohan | Bengaluru
```

SCD Type 1 means we overwrite the old value.

```python
(
    target.alias("t")
    .merge(
        source.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

Result:

```text
101 | Mohan | Bengaluru
```

No historical version of Hyderabad is maintained in the business table.

---

# 21. Delta SCD Type 2

SCD Type 2 is different.

Instead of overwriting:

```text
101 | Mohan | Hyderabad | 2025-01-01 | 9999-12-31 | current
```

we close the old record:

```text
101 | Mohan | Hyderabad  | 2025-01-01 | 2026-08-16 | false
101 | Mohan | Bengaluru  | 2026-08-16 | 9999-12-31 | true
```

Delta `MERGE` is commonly used to implement this pattern.

---

# 22. Delta Table Interview Questions ⭐

### Q1. What is a Delta table?

> A Delta table is a table that uses Delta Lake's transaction log on top of data files, typically Parquet, to provide features such as ACID transactions, schema enforcement, time travel, and reliable UPDATE/DELETE/MERGE operations.

### Q2. What is `_delta_log`?

> It is the transaction log that records changes to the Delta table and allows Delta to determine the table's state at different versions.

### Q3. Is Delta a file format?

**Best answer:**

> Delta is better described as a table/storage layer built around Parquet data files and a transaction log, rather than simply another file format like Parquet.

### Q4. Can Delta update individual records?

Yes, logically. Delta handles the underlying file-level changes.

### Q5. What is MERGE used for?

> Upserts, CDC processing, synchronization, SCD implementations, and matching source records against target records.

### Q6. What is time travel?

> The ability to query previous versions of a Delta table using a version number or timestamp.

### Q7. What is VACUUM?

> VACUUM physically removes old files that are no longer required by the table after the applicable retention period.

### Q8. VACUUM vs DELETE?

```text
DELETE
→ removes rows logically from the current table state

VACUUM
→ physically removes obsolete files
```

### Q9. OPTIMIZE vs VACUUM?

```text
OPTIMIZE
→ improves file layout / compaction

VACUUM
→ removes obsolete files
```

### Q10. Delta vs Iceberg?

Both are modern **open table formats/table-storage technologies** that provide features such as ACID transactions, schema evolution and time travel, but their transaction metadata and ecosystem implementations differ.

---

# 23. Most Important Delta Concepts for a PySpark Interview

If you're preparing specifically for a **Data Engineering/PySpark interview**, prioritize these:

```text
⭐⭐⭐⭐⭐ MERGE
⭐⭐⭐⭐⭐ ACID transactions
⭐⭐⭐⭐⭐ _delta_log
⭐⭐⭐⭐⭐ Time Travel
⭐⭐⭐⭐⭐ Schema Enforcement
⭐⭐⭐⭐⭐ Schema Evolution
⭐⭐⭐⭐ UPDATE / DELETE
⭐⭐⭐⭐ VACUUM
⭐⭐⭐⭐ OPTIMIZE
⭐⭐⭐⭐ SCD Type 1
⭐⭐⭐⭐ SCD Type 2
⭐⭐⭐ CDC
⭐⭐⭐ Z-Ordering
```

### One-line mental model

```text
Parquet
   +
Delta Transaction Log
   ↓
Delta Table
   ↓
ACID + MERGE + UPDATE + DELETE
     + Time Travel
     + Schema Management
     + Reliable Data Pipelines
```

For your **PySpark/Data Engineering interview preparation**, the most important thing to understand deeply is **`MERGE + CDC + SCD Type 1/2 + `_delta_log` + Time Travel + VACUUM`**.
