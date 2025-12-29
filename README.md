# Real-Time E-Commerce Data Lake & Analytics Platform

An end-to-end **event-driven data lake and analytics system** built using **Kafka, Spring Boot, AWS S3, Glue, Athena, Airflow, and Metabase**.  
This project demonstrates a **production-style Bronze → Silver → Gold architecture** with orchestration, schema evolution, and BI dashboards.

---

## 📌 Architecture Overview

**Flow**
Producers → Kafka → Spring Consumer → S3 (Bronze)
→ AWS Glue Crawler
→ Athena CTAS (Silver)
→ Aggregations (Gold)
→ BI Dashboard (Metabase)


**Design Principles**
- Event-driven ingestion
- Immutable raw data (Bronze)
- Curated analytics-ready data (Silver)
- Business aggregates (Gold)
- Fully containerized local setup
- Cloud-native analytics (serverless)

---

## 🧩 Tech Stack

### Data Ingestion
- **Apache Kafka** – Event streaming platform
- **Spring Boot (Java 17)** – Kafka consumer, S3 writer
- **Python** – Kafka event producer (mock data)

### Data Lake & Processing
- **Amazon S3** – Bronze, Silver, Gold layers
- **AWS Glue Crawler** – Schema discovery & cataloging
- **Amazon Athena** – SQL-based analytics (CTAS)

### Orchestration
- **Apache Airflow (Dockerized)**  
  - Glue Crawlers  
  - Athena CTAS queries  
  - Dependency management & retries  

### BI & Analytics
- **Metabase** – Dashboarding & visualization
- **Athena JDBC** – Direct analytics on S3 data

### Platform & Tooling
- **Docker & Docker Compose**
- **AWS IAM (least privilege)**
- **GitHub-ready project structure**

---

## 📂 Data Lake Layers

### 🥉 Bronze (Raw)
- One folder per Kafka topic
- JSON events exactly as produced
- Immutable, append-only

---

### 🥈 Silver (Curated)
- Normalized schema
- Partitioned by `date` and `hour`
- Stored as **Parquet**
- Created using **Athena CTAS**

---

### 🥇 Gold (Business Aggregates)

Examples:
- Orders Daily Summary
- Payments Daily Summary
- User Activity Summary

ecommerce_gold.orders_daily_summary
ecommerce_gold.payment_daily_summary
ecommerce_gold.user_daily_activity_summary

---

## 🛠 Airflow DAG

**Pipeline Steps**
1. Run Bronze Glue Crawler
2. Create Silver table (CTAS)
3. Run Silver Glue Crawler
4. Create Gold aggregate tables
5. Retry-safe & cost-aware execution

**Key Features**
- Deferrable Glue operators
- No duplicate crawler execution
- Cost-optimized retries
- Fully Dockerized

---

## 📊 Dashboards (Metabase)

Built dashboards include:
- Orders per day
- Revenue trends
- Payment success/failure ratio
- User activity metrics

Metabase connects directly to **Athena**, querying data stored in **S3 (Gold layer)**.

---

## 🔐 Secrets & Configuration

Secrets are **never committed**.

### Used locally via:
- `.env` file
- Docker environment variables

### Protected using:
- `.gitignore`
- `.env.example` for reference

---

## 🚀 How to Run (High Level)

## 🚀 How to Run the Project (Step-by-Step)

Follow the steps **in order**. Each layer is started independently for clarity and control.

---

## 🔐 Prerequisites

Before starting **any container**, you **must** provide AWS credentials.

Create a `.env` file (this file is **NOT committed to Git**):

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=eu-north-1
```
These credentials are required for:

- Writing to S3
- Running Glue Crawlers
- Executing Athena queries
- Connecting Metabase to Athena

1️⃣ Start Kafka, Zookeeper, Producer & Spring Consumer
Go inside the main project directory:
cd datalake

Start core ingestion services:
```docker compose up -d```

This will start:

Zookeeper, Kafka, Python Kafka Producer, Spring Boot Kafka Consumer

Verify logs:
```
docker logs -f python-producer
docker logs -f spring-consumer
```

Verify Bronze Layer

Check your S3 bucket:

s3://ecommerce-event-bronze/


You should see topic-wise folders with JSON events.

2️⃣ Start Apache Airflow (Orchestration)
Go inside the Airflow directory:
cd airflow

Start Airflow services:
```
docker compose -f docker-compose.airflow.yml up -d
```

Create Airflow Admin User:
```
docker compose -f docker-compose.airflow.yml run --rm airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

Open Airflow UI:
http://localhost:8080/home

Login with:

Username: admin
Password: admin

3️⃣ Run the Airflow DAG

Open Airflow UI

Enable the DAG:

ecommerce_bronze_silver_gold_pipeline


Trigger the DAG manually

What the DAG does:

Runs Bronze Glue Crawler

Creates Silver table (Athena CTAS)

Runs Silver Glue Crawler

Creates Gold aggregate tables

Verify in AWS:

Glue Catalog → Databases & Tables

Athena → ecommerce_silver and ecommerce_gold

4️⃣ Start Metabase (BI Dashboard)
Go inside the Metabase directory:
cd metabase

Start Metabase:
```
docker compose -f docker-compose.metabase.yml up -d
```

Open Metabase UI:
http://localhost:3000

5️⃣ Connect Metabase to Amazon Athena

During Metabase setup:

Database: Amazon Athena

Region: eu-north-1

S3 Staging Directory:

s3://athena-query-results-nithinraaj-eu-north-1/


Workgroup: primary

Authentication: Use environment variables (already provided via .env)

After connection:

Sync schema

Select ecommerce_gold tables

6️⃣ View Dashboards

Once connected:

http://localhost:3000/dashboard


You will see:

Orders Daily Summary

Payments Summary

User Activity Metrics

✅ Final Result

✔ Real-time ingestion via Kafka
✔ Bronze → Silver → Gold data lake
✔ Orchestrated using Airflow
✔ Serverless analytics with Athena
✔ BI dashboards using Metabase

⚠️ Important Notes

Never commit .env files

Stop services when not in use to avoid AWS cost

Glue Crawlers are deferrable to prevent duplicate runs

🧠 Resume Value

This project demonstrates:

Real-world data lake architecture

Production-style orchestration

Cost-aware AWS usage

End-to-end ownership (ingest → BI)

---

## 💡 Why This Architecture?

- **Scalable** – Event-driven & serverless analytics
- **Cost-efficient** – No always-on clusters
- **Production-aligned** – Industry-standard data lake pattern
- **Resume-ready** – Real-world tools & design decisions

## 👤 Author

**Nithinraaj**  
Software Engineer | Backend & Data Engineering  
Focused on scalable, real-world system design

---

⭐ If this project helped you understand modern data platforms, give it a star!

