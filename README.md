# 🌊 IceStream – Real-Time Lakehouse Observability

## 📌 Project Overview

IceStream is a Real-Time Lakehouse Observability project designed to monitor data quality in streaming e-commerce transactions.

In traditional data pipelines, data quality problems such as NULL values, missing fields, and incorrect values may only be identified after dashboards or reports are affected.

IceStream addresses this problem by continuously processing transaction events, validating incoming data, separating valid and invalid records, and displaying data quality metrics through a Streamlit dashboard.

This project demonstrates a local MVP of a real-time data quality monitoring system.

## 🎯 Project Objective

The main objectives of this project are:

- To generate real-time e-commerce transaction data.
- To process streaming events using a Kafka-compatible message broker.
- To identify invalid or incorrect records.
- To separate valid and quarantined records.
- To store processed data in Parquet format.
- To monitor data quality using a dashboard.
- To generate downloadable data quality reports.

## 🛠️ Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data processing and validation |
| Redpanda | Kafka-compatible streaming message broker |
| Docker | Running Redpanda locally |
| Pandas | Data manipulation and analysis |
| PyArrow | Parquet file processing |
| Parquet | Data storage format |
| Streamlit | Interactive dashboard |
| Git & GitHub | Version control and project hosting |

## 🏗️ Project Architecture

The project follows this workflow:

**Producer → Redpanda → Consumer → Data Validation → Parquet Storage → Streamlit Dashboard**

### Architecture Explanation

1. **Producer:** Generates sample e-commerce checkout transaction events.
2. **Redpanda:** Receives and transfers transaction messages through the `checkout-events` topic.
3. **Consumer:** Reads incoming messages from Redpanda.
4. **Data Validation:** Checks transaction records using Python quality rules.
5. **Valid Records:** Correct records are stored in `valid_transactions.parquet`.
6. **Quarantined Records:** Invalid records are stored in `quarantine.parquet`.
7. **Dashboard:** Displays transaction metrics, quality alerts, issue analysis, and downloadable reports.

## 📂 Project Folder Structure

```text
IceStream_Full_MVP/
│
├── producer.py
├── consumer.py
├── dashboard.py
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .gitignore
│
└── data/
    ├── valid_transactions.parquet
    └── quarantine.parquet
```

## ⚙️ Project Features

### 1. Real-Time Data Generation

The Producer generates sample e-commerce transaction events containing:

- Transaction ID
- Event Time
- Customer ID
- Product
- Amount
- Tax Amount
- Currency

It also generates sample invalid data to test the data quality rules.

### 2. Streaming Data Processing

Redpanda acts as a Kafka-compatible message broker. It receives transaction events from the Producer and makes them available to the Consumer.

### 3. Data Quality Validation

The Consumer checks incoming records for:

- Missing required fields
- NULL tax values
- Negative transaction amounts
- Invalid tax values
- Unexpected fields or schema changes

### 4. Data Quarantine

Records that fail validation are separated and stored in a quarantine Parquet file.

Valid records are stored separately for further analysis.

### 5. Streamlit Dashboard

The dashboard displays:

- Total processed records
- Valid records
- Quarantined records
- Bad-data rate
- Data quality score
- Quality alerts
- Issue-wise analysis

### 6. Data Quality Reports

The dashboard also provides:

- Overall data quality summary
- Issue-wise error analysis chart
- Downloadable quarantined records in CSV format
- Downloadable overall quality summary in CSV format

## 💻 System Requirements

- Windows 10 or Windows 11
- Python
- Visual Studio Code
- Docker Desktop
- Git
- Internet connection for initial package and Docker image installation

## 🚀 Installation and Execution

### Step 1: Download the Project

Download or clone the project repository from GitHub.

### Step 2: Open Project Folder

Open the project folder in Visual Studio Code.

Open the VS Code terminal and navigate to the folder containing `docker-compose.yml`.

### Step 3: Start Docker Desktop

Make sure Docker Desktop is running.

### Step 4: Start Redpanda

Run the following command:

```powershell
docker compose up -d
```

This starts the Redpanda container.

### Step 5: Activate Virtual Environment

If `.venv` is located in the parent folder, run:

```powershell
..\.venv\Scripts\Activate.ps1
```

If `.venv` is inside the project folder, run:

```powershell
.\.venv\Scripts\Activate.ps1
```

### Step 6: Install Required Packages

```powershell
python -m pip install streamlit pandas pyarrow confluent-kafka
```

### Step 7: Run Producer

Open a new terminal in the project folder and execute:

```powershell
python producer.py
```

The Producer starts generating transaction events.

### Step 8: Run Consumer

Open another terminal and execute:

```powershell
python consumer.py
```

The Consumer reads events, validates records, and stores valid and invalid data separately.

### Step 9: Run Streamlit Dashboard

Open another terminal and execute:

```powershell
python -m streamlit run dashboard.py
```

Open the dashboard in your browser:

http://localhost:8501

## 📊 Dashboard Output

The dashboard provides a real-time view of the processed transaction data whenever it is refreshed.

### Main Dashboard Metrics

- Total Processed: Total number of processed transactions.
- Valid Records: Number of records that passed validation.
- Quarantined Records: Number of records that failed validation.
- Bad-Data Rate: Percentage of invalid records.

### Data Quality Report

The report provides:

- Overall data quality score.
- Valid and invalid record counts.
- Issue-wise error analysis.
- CSV download options.

## 📈 Expected Results

The project demonstrates how invalid transaction data can be identified and separated from valid data.

For example, if a transaction contains a NULL tax value or a negative amount, the Consumer identifies the issue and stores that record in the quarantine file.

The dashboard then displays the processed record counts and data quality metrics.

*Note: Dashboard values change according to the generated and processed transaction events.*

## 🔍 Testing and Validation

The project can be tested by:

1. Starting the Redpanda container.
2. Running the Producer.
3. Running the Consumer.
4. Opening the Streamlit dashboard.
5. Checking valid and quarantined records.
6. Verifying the quality summary.
7. Downloading CSV reports.

## ⚠️ Current Implementation Scope

This repository contains a local MVP built with:

- Redpanda for Kafka-compatible streaming.
- Python for data quality validation.
- Parquet for storing valid and quarantined records.
- Streamlit for monitoring and reporting.

The following features from the broader project proposal are **not yet implemented**:

- Apache Flink stream processing.
- Apache Iceberg lakehouse integration.
- React-based monitoring interface.
- Automatic circuit breaker and downstream pipeline pause.
- Automated data re-fetch and recovery.
- WebSocket-based dashboard updates.

These are planned future enhancements.

## 🔮 Future Enhancements

- Integrate Apache Flink for continuous stateful stream processing.
- Integrate Apache Iceberg for ACID lakehouse table management.
- Implement automatic circuit-breaker functionality.
- Add automated data recovery and re-fetch mechanisms.
- Develop a React-based observability interface.
- Add real-time dashboard updates using WebSockets.
- Maintain an incident history and audit log.

## 🎓 Learning Outcomes

Through this project, I gained practical experience in:

- Python programming.
- Streaming data concepts.
- Kafka-compatible messaging with Redpanda.
- Data quality validation.
- Handling invalid data.
- Parquet data storage.
- Building dashboards using Streamlit.
- Docker-based local development.
- Git and GitHub version control.

## 📝 Conclusion

IceStream demonstrates a local real-time data quality monitoring pipeline for e-commerce transaction data.

It generates streaming events, validates incoming records, separates valid and invalid transactions, stores them in Parquet files, and presents data quality metrics through an interactive dashboard.

The project provides a foundation for developing a more advanced real-time lakehouse observability system.

## 👩‍💻 Project Repository

**GitHub Repository:**

https://github.com/MasimukkuSravani14/Ice-Stream

---

**Project Name:** IceStream – Real-Time Lakehouse Observability

**Domain:** Data Architecture & Reliability Engineering

**Project Type:** Data Engineering / Data Quality Monitoring

**Status:** Local MVP Completed
