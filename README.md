# 🏦 Banking Data Pipeline: ETL, Data Moderation & Spark Job Optimization

An end-to-end **ETL pipeline for banking data** built with Apache Spark. It ingests raw transaction data, validates and moderates it (data quality checks, bad-record handling, sensitive-field masking), transforms it into analytics-ready tables, and applies **Spark job optimization** techniques to cut runtime and cost.

> ⚠️ Status: edit the sections below to match your actual code, tools and results.

## 📌 Project Overview

Banking systems generate huge volumes of data: customers, accounts, transactions, loans and cards. Raw data is often messy, duplicated, late-arriving or contains sensitive fields. This project builds a reliable pipeline that:

1. Extracts data from source files or databases
2. Moderates it: validates, cleans, quarantines bad records and masks sensitive data
3. Transforms it into curated, analytics-ready datasets
4. Loads it into a warehouse or data lake
5. Optimizes the Spark jobs for speed and resource usage

## 🏗️ Architecture

```
Source data (CSV / DB / API)
        │
        ▼
 ┌──────────────┐     ┌────────────────────┐     ┌──────────────────┐
 │  Raw / Bronze │ ──► │  Moderation / Silver│ ──► │  Curated / Gold  │
 │  ingestion    │     │  validate, clean,   │     │  aggregates,     │
 │               │     │  mask, deduplicate  │     │  reporting tables│
 └──────────────┘     └────────────────────┘     └──────────────────┘
                              │
                              ▼
                      Quarantine (bad records)
```

## ⭐ Features

### ETL
- Batch ingestion of customer, account and transaction data
- Schema enforcement and type casting
- Deduplication and null handling
- Joins and aggregations (daily balances, monthly spend, account summaries)
- Partitioned Parquet output

### Data Moderation
- Data quality rules (valid amounts, valid dates, non-null keys, allowed status values)
- Bad records routed to a quarantine table instead of failing the job
- Masking of sensitive fields (account number, card number, PII)
- Audit counts: records read, passed, rejected

### Spark Job Optimization
- Partition pruning and sensible `repartition` / `coalesce`
- Broadcast joins for small dimension tables
- Caching and persisting reused DataFrames
- Adaptive Query Execution (AQE) and skew handling
- Avoiding small files problem
- Using columnar formats (Parquet) and predicate pushdown
- Reading the Spark UI and execution plans to find bottlenecks

## 📈 Optimization Results

| Metric | Before | After |
|---|---|---|
| Job runtime | – | – |
| Shuffle data | – | – |
| Output files | – | – |
| Peak memory | – | – |

*Fill in your actual measurements.*

## 🛠️ Tech Stack

- Apache Spark / PySpark
- Python
- Parquet
- SQL
- *(Add what you used: Airflow, Delta Lake, Hive, AWS S3, Databricks, etc.)*

## 🚀 Getting Started

```bash
git clone https://github.com/amoltathesoftwareengineer/Banking-Data-Pipeline-ETL-Moderation-Spark-Job-Optimization.git
cd Banking-Data-Pipeline-ETL-Moderation-Spark-Job-Optimization
pip install -r requirements.txt
```

Run the pipeline:

```bash
spark-submit src/etl_job.py --input data/raw --output data/curated
```

## 📁 Project Structure

```
Banking-Data-Pipeline-ETL-Moderation-Spark-Job-Optimization/
├── data/
│   ├── raw/            # input files
│   ├── curated/        # output tables
│   └── quarantine/     # rejected records
├── src/
│   ├── ingest.py       # extract
│   ├── moderate.py     # validation, masking, quarantine
│   ├── transform.py    # business transformations
│   └── etl_job.py      # main entry point
├── tests/
├── requirements.txt
└── README.md
```

## 🔮 Future Improvements

- Incremental loads and change data capture (CDC)
- Streaming ingestion with Kafka and Spark Structured Streaming
- Orchestration with Airflow
- Data quality framework (Great Expectations / Deequ)
- Monitoring and alerting on pipeline metrics

## 🤝 Contributing

Contributions are welcome. Fork the repo, create a branch, commit your changes and open a pull request.

## 📄 License

MIT License. See [LICENSE](https://github.com/amoltathesoftwareengineer/Banking-Data-Pipeline-ETL-Moderation-Spark-Job-Optimization/blob/main/LICENSE).

## 👤 Author

Amol Tathe · [LinkedIn](https://linkedin.com/in/amoltathesoftwareengineer) · [GitHub](https://github.com/amoltathesoftwareengineer)
