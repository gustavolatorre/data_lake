# DATA LAKE

This project utilizes Docker to provide a streamlined environment for building a data lake and managing data workflows.

---

## Prerequisites

Before you begin, ensure you have **Docker** installed on your system.

---

## How to Run the Project

Follow these steps to get the project up and running:

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/gustavolatorre/data_lake.git
    ```

2.  **Navigate to the Project Root:**

    ```bash
    cd <your-project-folder>
    ```

3.  **Generate a Fernet Key:**

    Execute the following command in your terminal to generate a Fernet key, which is essential for Airflow's security:

    ```bash
    docker run --rm apache/airflow:2.11.0-python3.12 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    ```

4.  **Add Fernet Key to `airflow.env`:**

    Copy the key displayed in your terminal and paste it into the `airflow.env` file, assigning it to the `AIRFLOW__CORE__FERNET_KEY` variable:

    ```
    AIRFLOW__CORE__FERNET_KEY=<your-generated-fernet-key>
    ```

5.  **Create `.env` File:**

    Create a file named `.env` in the project's root directory and add the following environment variables. These will be your credentials for the Airflow and MinIO services. Replace the placeholder values with your desired credentials.

    ```
    AIRFLOW_USER=airflow
    AIRFLOW_PASSWORD=airflow
    AIRFLOW_EMAIL=your-email@gmail.com

    MINIO_ROOT_USER=admin
    MINIO_ROOT_PASSWORD=password
    MINIO_ENDPOINT=minio:9000
    ```

6.  **Build Images and Run the Project:**

    Execute the following command in your terminal to build the necessary Docker images and start the project in detached mode:

    ```bash
    docker-compose up --build -d
    ```

7.  **Access the Services:**

    Once the services are running, you can access them in your web browser:

    * **MinIO:** http://localhost:9001/
    * **Airflow:** http://localhost:8080/
    * **Spark Master:** http://localhost:9090/

8.  **Restart Airflow Webserver (if needed):**

    If Airflow does not start automatically, you can restart its webserver with the following command:

    ```bash
    docker-compose restart webserver
    ```

9.  **Stop the Project:**

    To stop all running services and remove the Docker volumes, execute:

    ```bash
    docker-compose down -v
    ```

---

## Architecture

This project is orchestrated using **Apache Airflow** and leverages **Docker Compose** to manage various services. Data flows through a medallion architecture (Bronze, Silver, Gold layers) stored in **MinIO**, processed by **Apache Spark**, and managed by **Apache Hudi**.

### Components

* **Apache Airflow**:
    * **Orchestration**: Airflow is used to schedule and monitor the data pipeline. The `breweries_pipeline.py` DAG defines the workflow.
    * **Scheduler**: Manages and triggers DAGs.
    * **Webserver**: Provides a user interface to monitor and manage DAGs.
    * **Executor**: Tasks are executed using the `spark_docker` connection to submit Spark jobs.
    * **Dockerfile.airflow**:
        * `apache/airflow:2.11.0-python3.12`: Base Airflow image.
        * `openjdk-17-jdk`: Installed to support Spark jobs.
        * `minio`, `apache-airflow-providers-apache-spark`, `pyspark==3.5.2`: Python packages installed for MinIO interaction, Spark integration, and PySpark execution within Airflow.

* **MinIO**:
    * **Object Storage**: Acts as an S3-compatible object storage for all data layers (Bronze, Silver, Gold).
    * **`minio` service**: The core MinIO server.
    * **`minio-setup` service**: Initializes MinIO by creating the `bronze`, `silver`, and `gold` buckets on startup, ensuring the data lake structure is ready.

* **Apache Spark**:
    * **Distributed Processing**: Used for data ingestion and transformation.
    * **`spark-master`**: The main node of the Spark cluster.
    * **`spark-worker`**: Worker nodes that execute tasks distributed by the Spark master.
    * **PySpark**: The `aggregate_breweries.py` and `breweries_bronze_to_silver.py` scripts are PySpark applications that run on the Spark cluster.

* **PostgreSQL**:
    * **Airflow Metadata Database**: Stores Airflow's metadata, including DAG definitions, task states, and historical runs.

* **Apache Hudi**:
    * **Lakehouse Format**: Used in the Silver layer (`breweries_bronze_to_silver.py`) to manage data as a Lakehouse. This allows for efficient upserts and versioning of data.

### Data Flow

1.  **Bronze Layer Ingestion (`breweries_fetch_and_upload_to_bronze.py`)**:
    * The `breweries_fetch_and_upload_to_bronze` Python task (Airflow `PythonOperator`) fetches brewery data from the OpenBreweryDB API.
    * The raw JSON data is then uploaded to the `bronze` bucket in MinIO.

2.  **Silver Layer Transformation (`breweries_bronze_to_silver.py`)**:
    * The `breweries_bronze_to_silver` task (Airflow `SparkSubmitOperator`) reads the raw JSON data from the `bronze` layer in MinIO.
    * It performs data quality checks (e.g., handling nulls, validating schema) and transformations (e.g., cleaning state names, adding ingestion date).
    * The processed data is then written to the `silver` bucket in MinIO as an Apache Hudi table, using `id` as the record key and `state` as the partition field, with an "upsert" operation.

3.  **Gold Layer Aggregation (`aggregate_breweries.py`)**:
    * The `aggregate_breweries` task (Airflow `SparkSubmitOperator`) reads the Hudi table from the `silver` layer.
    * It aggregates the data, counting the number of breweries by `brewery_type` and `state`.
    * The aggregated data is then written to the `gold` bucket in MinIO as Parquet files, replacing the existing table (`overwrite` mode).

### Docker Compose Services

The `docker-compose.yml` file defines the following services:

* **`spark-master`**: Spark master node.
* **`spark-worker`**: Spark worker node(s) connected to the master.
* **`postgres`**: PostgreSQL database for Airflow metadata.
* **`webserver`**: Airflow web interface.
* **`scheduler`**: Airflow scheduler.
* **`minio`**: MinIO object storage server.
* **`minio-setup`**: A utility container that ensures the `bronze`, `silver`, and `gold` buckets are created in MinIO upon startup.

This setup provides a complete, containerized environment for running the data pipeline, from data ingestion to aggregated analytics.