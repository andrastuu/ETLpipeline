#  ETL Pipeline

## 📌 Overview
This project is a modular, cloud-ready **ETL pipeline** built as part of a data engineering assessment. The goal was to build a robust, scalable, and easily testable data pipeline that can unify internal user conversion data with broker data for more effective product analysis and decision-making.

---

## 🛠 Stack & Architecture

| Component           | Purpose                                                                      |
|--------------------|-------------------------------------------------------------------------------|
| **Python**          | Core scripting language for ETL logic                                         |
| **Airflow**         | Orchestrates the DAG (extract → transform → load → profile) with visibility   |
| **Docker**          | Isolates the environment for local development and future CI/CD portability   |
| **S3**              | Serves as a scalable, cloud-native destination for final datasets             |
| **SQLite**          | Simple local database format for lightweight querying                         |
| **YData Profiling** | Fast, automated exploratory data analysis                                     |
| **Pytest**          | Unit testing to validate transformations and data integrity                   |

---

## 🔄 ETL Flow Diagram

![ETL Flow Diagram](flowchart.drawio.png)

---

## 📊 Pipeline Breakdown

### 1. **Extract**
- Ingests CSV files using patterns to support batch loads.
- Normalizes date formats and delimiter types (semicolon or comma).
- Loads a category mapping CSV and region mapping YAML.
- Ensures critical columns are present before proceeding.

### 2. **Transform**
- Cleans and harmonizes country names using a YAML-based mapping.
- Maps UI event categories using a `page_category_mapping.csv` file.
  - Adds a `was_matched` flag indicating whether a conversion matched broker data.
- Matches broker data to user conversions using timestamp proximity within a configurable tolerance.
- Exported data includes full match results and a profiling-ready format.
  - Exports unmatched rows to a separate file for investigation.
- All transformations are tested and validated using Pytest and Pandera.

### 3. **Load**
- Saves output to:
  - CSV (flat file)
  - SQLite (lightweight local DB)
  - AWS S3 (cloud storage for scale & accessibility)

### 4. **Profile**
- Uses YData Profiling to generate an interactive HTML report.
- Helps quickly identify missing data, distributions, and correlation patterns.
- Used for initial EDA and ongoing pipeline quality control.

---

## 🔁 Scalability & Cost Considerations

This pipeline is designed to be modular and portable. Each stage can be scaled independently, whether for more compute power, batch frequency, or output destinations.

### 🧹 Modular Design Benefits
- Tasks can be re-ordered, removed, or triggered independently.
- Suitable for extension into event-driven or micro-batch processing.

### 💰 Estimated Cloud Storage Cost (S3)

Assuming an average row size of 1 KB:

| Rows         | Approx Size | Monthly Cost (S3 Standard) |
|--------------|-------------|-----------------------------|
| 100,000      | ~100 MB     | ~$0.0023                    |
| 1,000,000    | ~1 GB       | ~$0.023                     |
| 10,000,000   | ~10 GB      | ~$0.23                      |
| 100,000,000  | ~100 GB     | ~$2.30                      |

> These are raw storage costs. Read/write requests and transfer costs would be additional but minimal for this use case.

### ⚙️ Runtime Cost (Airflow via MWAA)

When deployed using Amazon MWAA (Managed Airflow):

| Frequency     | Runtime/Month | Estimated Monthly Cost (MWAA + EC2) |
|---------------|----------------|--------------------------------------|
| Daily         | ~2.5 hours     | $20–30                                |
| Hourly        | ~30 hours      | $60–100                               |
| Event-based   | Varies         | Lower (with Lambda or Step Functions) |

> Disclaimer: Moving smaller steps (e.g. load or profiling) to AWS Lambda functions or even AWS Glue jobs can have better cost efficiency.

---

## ▶️ How to Run It

```bash
# Start the stack
docker compose up --build

# Access Airflow
Visit http://localhost:8080 (user: airflow, pass: airflow)

# Trigger the DAG
Run the DAG manually or on a schedule
```

Outputs:
- `output/final_output.csv`
- `output/unmatched_conversions.csv`
- `output/matched_data.sqlite`
- `output/profiling_report.html`
- S3 upload to: `s3://<bucket>/matched_data.csv`

---

## 📁 Project Structure
```
brokerchooser-etl/
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── profiling_task.py
│   └── aws.env
├── dags/
│   └── brokerchooser_dag.py
├── tests/
│   └── test_transform.py
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── regions.yml
└── output/
```

---

## 🧠 Why These Tools?
| Tool               | Reason                                                                 |
|--------------------|------------------------------------------------------------------------|
| **Airflow**         | Flexible, production-ready orchestration with dependency handling     |
| **Docker**          | Makes local testing and cloud migration seamless                      |
| **S3**              | Cheap, durable cloud storage with API access                          |
| **YData Profiling** | Saves hours of manual data exploration                               |
| **Pytest**          | Prevents silent errors by checking assumptions on input/output data   |
| **Pandera**         | Provides runtime schema validation to enforce structure + datatypes   |

---

## 📌 Final Notes

This project delivers a modular, testable, and cloud-adaptable pipeline suitable for production with minimal adjustments. Each component is isolated for clarity, and the whole system is compatible with modern orchestration and CI/CD pipelines.

---

**Author**: Andras Tuu  
**Date**: April 2025  
**Contact**: www.linkedin.com/in/andrás-tűű-99a0b61bb
