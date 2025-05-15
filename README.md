# 📡 Real-Time UPI Merchant Transaction Aggregation (CDC Pipeline)

This project demonstrates an **industrial-grade real-time Change Data Capture (CDC)** pipeline using **Databricks**, **Spark Structured Streaming**, and **Delta Lake**. It simulates UPI (Unified Payments Interface) transactions and aggregates merchant-level sales and refund activity using **streaming CDC feeds**.

---

## 🚀 Objective

Build a real-time data pipeline that:

1. Ingests simulated UPI transactions into a Delta table.  
2. Enables **CDC tracking** to detect updates (e.g., completed/refunded).  
3. Streams CDC changes in **micro-batches** using Structured Streaming.  
4. Aggregates data **per merchant** (sales, refunds, net revenue).  
5. Maintains an **up-to-date aggregated Delta table** for fast analytics.

---

## ⚙️ Tech Stack

- **Databricks**
- **Apache Spark Structured Streaming**
- **Delta Lake**
- **Change Data Feed (CDF)**
- **Python (PySpark)**

---

## 📁 Project Structure

| Notebook | Purpose |
|---------|---------|
| `upi_merchant_pay_trx_mock_data.ipynb` | Creates the raw Delta table and simulates UPI transactions using mock data. Uses batch-based upserts (`MERGE INTO`) to simulate real-world CDC events. |
| `realtime_merchant_aggregation.ipynb` | Reads CDC feed from the raw table and performs merchant-level aggregations. Updates an `aggregated_upi_transactions` Delta table using `MERGE`. |

---

## 🛠️ How It Works

### ✅ 1. Source Table with CDC

A Delta table `raw_upi_transactions_v1` is created with:

```sql
TBLPROPERTIES ('delta.enableChangeDataFeed' = true)
```

This enables tracking of changes (`insert`, `update_preimage`, `update_postimage`, `delete`) for streaming.

---

### ✅ 2. Simulated Transaction Batches

Mock UPI transactions are inserted/updated across 3 batches:

- **Batch 1**: Initial inserts  
- **Batch 2**: Status updates (e.g., to `completed`/`failed`)  
- **Batch 3**: Refunds and late completions  

All changes are upserted to the source table using Delta Lake’s `MERGE INTO`.

---

### ✅ 3. Streaming CDC Reader

The CDC feed is consumed using:

```python
cdc_stream = spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .table("raw_upi_transactions_v1")
```

This allows structured streaming to process new and updated rows in near real-time.

---

### ✅ 4. Aggregation and Upsert Logic

Each micro-batch performs the following logic to compute merchant-level aggregations:

```python
aggregated_df = batch_df \
    .filter(col("_change_type").isin("insert", "update_postimage")) \
    .groupBy("merchant_id") \
    .agg(
        sum(when(col("transaction_status") == "completed", col("transaction_amount")).otherwise(0)).alias("total_sales"),
        sum(when(col("transaction_status") == "refunded", -col("transaction_amount")).otherwise(0)).alias("total_refunds")
    ) \
    .withColumn("net_sales", col("total_sales") + col("total_refunds"))
```

These results are merged into a Delta table:

```python
DeltaTable.forName(spark, "aggregated_upi_transactions") \
    .alias("target") \
    .merge(aggregated_df.alias("source"), "target.merchant_id = source.merchant_id") \
    .whenMatchedUpdate({
        "total_sales": "target.total_sales + source.total_sales",
        "total_refunds": "target.total_refunds + source.total_refunds",
        "net_sales": "target.net_sales + source.net_sales"
    }) \
    .whenNotMatchedInsertAll() \
    .execute()
```

---

## 📈 Example Output

The `aggregated_upi_transactions` Delta table holds:

| merchant_id | total_sales | total_refunds | net_sales |
|-------------|-------------|---------------|-----------|
| M001        | 500.0       | -500.0        | 0.0       |
| M002        | 0.0         | 0.0           | 0.0       |
| M003        | 1500.0      | 0.0           | 1500.0    |

---

## 🔄 Benefits of Delta Lake CDC

- Fine-grained incremental processing  
- Low-latency stream updates  
- Supports **MERGE**, **DELETE**, **UPDATE** use cases  
- Ideal for **real-time reporting** and **transactional audit trails**

---

## 📌 Future Enhancements

- Add **schema evolution** handling  
- Use **Watermarking** for late-arriving data  
- Persist raw change logs for audit compliance  
- Integrate with **Power BI** or **Tableau** for dashboards  

---

## 📬 Contact

For queries or collaboration, feel free to raise an issue or open a PR!

---