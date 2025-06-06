# 🚀 Realtime Data Streaming Project

## 📘 Introduction

This is a hands-on project that builds an end-to-end **data engineering pipeline**. It covers every stage — from **data ingestion** to **processing** and **storage**, using a modern tech stack


All components are containerized using **Docker**.

---

## 🏗️ System Architecture

![System Architecture](Project_architecture.png)



---

## 🛠️ Technologies Used

- **Apache Airflow**
- **Python**
- **Apache Kafka**
- **Apache Zookeeper**
- **Apache Spark**
- **MongoDB**
- **Google BigQuery**
- **PostgreSQL**
- **Docker**

---

## 🔄 Pipeline Flow

1. **Data Source**  
    Uses `randomuser.me` API to generate random user data.

2. **Apache Airflow**  
   Orchestrates the pipeline and sends fetched data to Kafka.

3. **Apache Kafka & Zookeeper**  
   Stream data from Airflow to be processed by Apache Spark.

4. **Control Center & Schema Registry**  
   Monitor Kafka and manage schemas.

5. **Apache Spark**  
   Processes data from Kafka using Structured Streaming and writes it to MongoDB and Google BigQuery.

6. **MongoDB**  
   Store raw data for fast document-based access.

7. **BigQuery**  
   Store structured data for analysis and visualization.

---

## 🚀 Steps to Start

### 1. Clone the Repository

```bash
git clone https://github.com/Youssefalaa99/Data-Engineering-Project.git
```

### 2. Activate Virtual Environment
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install Required Packages
```bash
pip install -r requirements.txt
```

### 4. Run Docker Services
```bash
docker-compose up -d
```

---

## 🌐 Interface Links
### Airflow UI
- **URL**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`

### Kafka Confluent Control Center
- **URL**: http://localhost:9021

### Spark Master UI
- **URL**: http://localhost:9090/

---

## ▶️ Running the pipeline
### Setup BigQuery Access
- Create a GCP service account from IAM
- Give the service account required permissions
   - BigQuery Data Editor
   - BigQuery Job User
- Create and download JSON key file.
- Set env variable before running pipeline
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS='<key_path.json>'
   ```
- Configure project_id and dataset in spark/src/config/setting.py

### Run Airflow to extract API data
- Go to Airflow Web UI.
- Unpause **stream_to_kafka** DAG.

### Run pyspark streaming application 
#### Method 1: Direct Python Run
```bash
python spark/src/main.py
```
#### Method 2: Spark Submit
```bash
spark-submit --master spark://localhost:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.2 \
spark/src/main.py
```

### Check loaded data
#### From mongosh
```bash
db.users.find()
```
#### From BigQuery SQL editor
```sql
SELECT * FROM `project_id.dataset.users`
```

---

## 📎 Reference

Special thanks to **CodewithYu** for the project inspiration.  
🎥 [Original Project Video](https://www.youtube.com/watch?v=GqAcTrqKcrY&t=1s)
