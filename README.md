# DATA LAKE

This project uses Docker to provide a streamlined environment for building a data lake and managing data workflows.

---

## 📦 Prerequisites

Before starting, make sure **Docker** is installed on your system.

---

## 🚀 How to Run the Project

Follow these steps to get the project running:

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/gustavolatorre/data_lake.git
   ```

2. **Navigate to the project root:**

   ```bash
   cd data_lake
   ```

3. **Generate a Fernet Key:**

   Run the following command to generate a Fernet key, required for Airflow encryption:

   ```bash
   docker run --rm apache/airflow:2.11.0-python3.12 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

4. **Add the Fernet Key to `airflow.env`:**

   Copy the key generated and paste it into the `airflow.env` file:

   ```env
   AIRFLOW__CORE__FERNET_KEY=<your-generated-key>
   ```

5. **Create the `.env` file:**

   In the root directory, create a `.env` file with your Airflow and MinIO credentials:

   ```env
   AIRFLOW_USER=airflow
   AIRFLOW_PASSWORD=airflow
   AIRFLOW_EMAIL=your-email@gmail.com
   MINIO_ROOT_USER=admin
   MINIO_ROOT_PASSWORD=password
   MINIO_ENDPOINT=minio:9000
   ```

6. **Build the images and start the project:**

   ```bash
   docker-compose up --build -d
   ```

7. **Access the services in your browser:**

   - **MinIO:** http://localhost:9001/
   - **Airflow:** http://localhost:8080/
   - **Spark Master:** http://localhost:9090/

8. **Restart Airflow Webserver (if needed):**

   ```bash
   docker-compose restart webserver
   ```

9. **Stop the project:**

   ```bash
   docker-compose down -v
   ```

---

## 🧱 Architecture

The project is orchestrated with **Apache Airflow** and uses **Docker Compose** to manage services. The data flows through a medallion architecture (Bronze, Silver, Gold), stored in **MinIO**, processed with **Apache Spark**, and managed with **Apache Hudi**.

### 🔧 Components

- **Apache Airflow**:
  - Orchestration of pipelines with the DAG `breweries_pipeline.py`

  - Scheduler, Webserver, and Executor configured

  - `spark_docker` connection to submit Spark jobs
  
  - Custom `Dockerfile.airflow` with:
    - `openjdk-17-jdk`
    - `minio`, `pyspark==3.5.2`, `apache-airflow-providers-apache-spark`

- **MinIO**:
  - S3-compatible object storage for bronze/silver/gold layers
  - `minio` service as the storage server
  - `minio-setup` to automatically create buckets

- **Apache Spark**:
  - Distributed data processing
  - `spark-master` and `spark-worker` services
  - PySpark scripts:
    - `aggregate_breweries.py`
    - `breweries_bronze_to_silver.py`

- **PostgreSQL**:
  - Metadata database for Airflow

- **Apache Hudi**:
  - Lakehouse format used in the Silver layer
  - Enables upserts and data versioning

---

## 🔄 Data Flow

1. **Bronze Ingestion (`breweries_fetch_and_upload_to_bronze.py`)**:
   - Uses `PythonOperator` to fetch data from OpenBreweryDB API
   - Stores raw JSON in the `bronze` bucket in MinIO

2. **Silver Transformation (`breweries_bronze_to_silver.py`)**:
   - Reads JSON from the Bronze layer
   - Applies validations and transformations
   - Writes Hudi table to `silver`, partitioned by `state`

3. **Gold Aggregation (`aggregate_breweries.py`)**:
   - Reads Hudi data from the Silver layer
   - Aggregates by `brewery_type` and `state`
   - Writes Parquet to the `gold` bucket in overwrite mode

---

## 🐳 Docker Compose Services

- `spark-master`: Spark master node
- `spark-worker`: Spark worker nodes
- `postgres`: Airflow metadata database
- `webserver`: Airflow web interface
- `scheduler`: Airflow DAG scheduler
- `minio`: MinIO object storage server
- `minio-setup`: Initializes `bronze`, `silver`, and `gold` buckets

---

This setup provides a complete, containerized environment for data ingestion, transformation, and analytics with Airflow + Spark + MinIO + Hudi.