# 🎵 Real-Time Spotify Data Engineering Pipeline

An end-to-end real-time data engineering pipeline that extracts playlist data from the Spotify API, transforms it using AWS Glue and PySpark, and loads it into Snowflake for analytics. The pipeline is fully automated with AWS EventBridge and AWS Lambda and leverages Snowflake Streams and Tasks for incremental processing and deduplication.

---

## 🚀 Architecture

```text
EventBridge
      ↓
AWS Lambda (Spotify API Extraction)
      ↓
Amazon S3 (Raw JSON)
      ↓
AWS Glue (PySpark Transformations)
      ↓
Amazon S3 (Parquet Files)
      ↓
Snowpipe
      ↓
Snowflake Staging Tables
      ↓
Snowflake Streams
      ↓
Snowflake Tasks (MERGE)
      ↓
Analytics Tables
```

---

## 🛠️ Tech Stack

- **Programming Languages:** Python, SQL
- **Big Data Processing:** PySpark
- **Cloud Services:** AWS Lambda, Amazon S3, AWS Glue, Amazon EventBridge
- **Data Warehouse:** Snowflake
- **Data Ingestion:** Snowpipe
- **Incremental Processing:** Snowflake Streams, Snowflake Tasks
- **Data Formats:** JSON, Parquet
- **Libraries:** boto3, spotipy, requests
- **Development Tools:** Git, GitHub, uv, Jupyter Notebook

---

## 📂 Project Structure

```text
Real-Time-Spotify-Data-Engineering-Pipeline/
│
├── Glue/
│   └── spotify_transformation_job.py
│
├── Lambda/
│   └── spotify_extract_lambda.py
│
├── Snowflake/
│   ├── spotify_snowflake.sql
│
├── notebooks/
│   ├── 01_api_exploration.ipynb
│   └── spotify_transformation_job.ipynb
│
├── README.md
├── pyproject.toml
├── uv.lock
└── .gitignore
```

---

## 🔄 Data Pipeline Flow

### 1. Data Extraction
- AWS Lambda authenticates with the Spotify API using a refresh token.
- Playlist data is extracted and stored as JSON files in Amazon S3.

### 2. Data Transformation
- AWS Glue reads raw JSON files from S3.
- PySpark transforms and normalizes the data into:
  - Albums
  - Artists
  - Songs
- Transformed data is written back to S3 in Parquet format.

### 3. Data Loading
- Snowpipe automatically loads Parquet files into Snowflake staging tables.

### 4. Incremental Processing
- Snowflake Streams capture newly loaded records.
- Snowflake Tasks automatically execute MERGE statements to maintain deduplicated analytics tables.

---

## ✨ Features

- Fully automated pipeline using EventBridge and Lambda
- Incremental ingestion every 5 minutes
- Real-time loading with Snowpipe
- Automated deduplication using Streams and Tasks
- Append-only staging architecture
- Fault-tolerant and replayable pipeline design

---

## 🏗️ Snowflake Architecture

```text
S3 Parquet Files
        ↓
     Snowpipe
        ↓
 Staging Tables
        ↓
      Streams
        ↓
       Tasks
        ↓
  Final Analytics Tables
```

---

## 📊 Example Workflow

```text
Initial Playlist: 35 Songs
       ↓
Add 2 New Songs
       ↓
Pipeline Executes Automatically
       ↓
Final Analytics Table: 37 Unique Songs
```

No duplicate records are introduced into the final analytics tables.

---

## 🔮 Future Improvements

- Infrastructure as Code (Terraform)
- CI/CD with GitHub Actions
- Data quality checks with Great Expectations
- Power BI or Tableau dashboards
- Monitoring and alerting with CloudWatch

---

## 📚 Key Learnings

- Building event-driven data pipelines on AWS
- Designing medallion-style architectures
- Implementing incremental loading patterns
- Using Snowflake Streams and Tasks for CDC processing
- Managing real-time data ingestion and deduplication

---

## 👨‍💻 Author

**Duc Anh Nguyen**

- LinkedIn: https://www.linkedin.com/in/ducanhng85/
- GitHub: https://github.com/ducanhng85
