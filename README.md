# US Airline Flight Routes and Fares Analytics

This is my final project for [Data Engineering Zoomcamp 2025.](https://github.com/DataTalksClub/data-engineering-zoomcamp?tab=readme-ov-file)

## Problem Description

This dashboard analyzes historical USA airline flight route data, containing details on pricing, carriers, and origin/destination pairs across different years. The visualizations aim to address the following key questions about the air travel market:

- How has the average airfare evolved over the years?

- Which origin cities offer the widest range of direct connections (measured by the percentage of unique destinations reached relative to all destinations)?

- Which destination cities are accessible from the broadest set of origin points (measured by the percentage of unique origins served relative to all origins)?

- How do pricing trends and connectivity metrics differ when analyzing specific origin-destination pairs or individual cities using the provided filters?

## Overview
- **Data source** - https://www.kaggle.com/datasets  
- **Workflow orchestration** - Airflow
- **Data lake** - Cloud Storage
- **Data warehouse** - Bigquery
- **Data transformation** - dbt
- **Data visualization** - Looker Studio

## Data pipeline
<img src="https://github.com/VMynenko/air-route-analytics/blob/main/docs/pipeline.png" alt="pipeline" width="500" />  

## Data source
The dataset used in this project is sourced from Kaggle and provides detailed information on airline flight routes, fares, and passenger volumes within the United States from 1993 to 2024.  
A detailed description and link to the dataset can be found [here.](https://www.kaggle.com/datasets/bhavikjikadara/us-airline-flight-routes-and-fares-1993-2024)

## Workflow orchestration
The data pipeline is orchestrated using Apache Airflow, which is deployed on Google Cloud Composer. Below are the essential bash commands used to create the Composer environment, deploy DAGs, install dependencies, and set Airflow variables.  
To replicate this setup, follow these steps:  
#### Create Cloud Composer environment  
```bash
export PROJECT_ID="your-gcp-project"
export REGION="your-region"
export ENV_NAME="composer-env"

gcloud composer environments create $ENV_NAME \
    --location $REGION \
    --image-version composer-2.11.5-airflow-2.10.2 \
    --project $PROJECT_ID
```
#### Deploy DAGs and requirements  
```bash
gcloud composer environments storage dags import \
    --environment $ENV_NAME \
    --location $REGION \
    --source your_dag.py

gcloud composer environments storage plugins import \
    --environment $ENV_NAME \
    --location $REGION \
    --source requirements.txt
```
#### Set Airflow variables  
```bash
airflow variables set dataset_name "kaggle-dataset-path"
airflow variables set bucket_name "your-gcs-bucket"
airflow variables set table_id "your-bq-table"
```
Screenshots of the deployment result in Google Cloud Console  
<img src="https://github.com/VMynenko/air-route-analytics/blob/main/docs/cloud_composer_1.png" alt="cc1" width="900" />  
***
<img src="https://github.com/VMynenko/air-route-analytics/blob/main/docs/cloud_composer_2.png" alt="cc2" width="900" /> 

The entire pipeline is executed as an Airflow DAG, which automates the data movement from Kaggle to BigQuery.  
The DAG code can be found [here.](https://github.com/VMynenko/air-route-analytics/blob/main/code/de_zoomcamp_2025_dag.py)

## Data Lake (Google Cloud Storage)    
The dataset is downloaded from Kaggle and stored in a Google Cloud Storage bucket using Airflow.  
#### Code Snippet
```python
def create_bucket(bucket_name, gcp_conn_id=GCP_CONN_ID):
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.create_bucket(bucket_name=bucket_name)

def upload_file(bucket_name, source_file, gcp_conn_id=GCP_CONN_ID):
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    destination_blob = os.path.basename(source_file)
    hook.upload(
        bucket_name=bucket_name,
        object_name=destination_blob,
        filename=source_file
    )
    return f"gs://{bucket_name}/{destination_blob}"
```
Screenshots of the code execution result in Google Cloud Console   
<img src="https://github.com/VMynenko/air-route-analytics/blob/main/docs/cloud_storage_1.png" alt="cs1" width="900" /> 
***
<img src="https://github.com/VMynenko/air-route-analytics/blob/main/docs/cloud_storage_2.png" alt="cs2" width="900" /> 

## Data Warehouse (BigQuery)  
The CSV file stored in Google Cloud Storage is loaded into a BigQuery table using Airflow.    
#### Code Snippet
```python
def load_gcs_to_bq(gcs_uri, table_id, gcp_conn_id="google_cloud_default"):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    bigquery_client = hook.get_client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True
    )
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    load_job.result()
```
Screenshot of the table creation result in Google Cloud Console   
<img src="https://github.com/VMynenko/air-route-analytics/blob/main/docs/bq_1.png" alt="bq" width="500" /> 

## Data transformation  
Data transformation is handled using dbt (Data Build Tool), which allows for SQL-based transformation and modeling of data in BigQuery.  
Below are the key steps to set up and run the dbt project.  
#### Create a new dbt project
```bash
dbt init my_final_project
cd my_final_project
```
#### Create a transformation model  
Create a new SQL file inside models/ (for example, transformed_airline_data.sql) and define the conversion logic using the script at this [link.](https://github.com/VMynenko/air-route-analytics/blob/main/dbt/transformed_airline_data.sql)

#### Run the dbt transformations
```bash
dbt run
```
This process materializes the transformed data as a table in BigQuery, ready for analysis in Looker Studio.

## Data visualization
