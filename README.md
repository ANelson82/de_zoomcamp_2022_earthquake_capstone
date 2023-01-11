# Earthquake Data Engineering Capstone Project

This project automatically ingests, stores, transforms the latest seismic activity data from the USGS (United States Geologic Society) for later analysis.

This GitHub repository fulfills the final capstone project for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks.Club](https://datatalks.club).

![Data Pipeline Architecture](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/architecture_earthquake.excalidraw.png)

# Dashboard
[Looker Studio](https://datastudio.google.com/reporting/ded3baf5-c5ce-455c-b5ad-e1d6be4c5cdd)

# Presentation
#todo
[Youtube Presentation](http://youtube.com)

# Technical Challenge
Analysts are studying the frequency, intensity, and spatial occurrence of seismic activity.  This data needs to be automatically stored and processed in a way that analysts can quickly analyze and build out reports and dashboards.  The technical implementation needs to be:
- Automated 
- Reliable
- Scalable
- Affordable 


# Technology Utilized
- **Infrastructure as code (IaC):** [Terraform](https://github.com/hashicorp/terraform)
- **Workflow orchestration:** [Airflow](https://airflow.apache.org/)
- **Containerization:** [Docker](https://www.docker.com/)
- **Data Lake:** [Google Cloud Storage (GCS)](https://cloud.google.com/storage)
- **Data Warehouse:** [BigQuery](https://cloud.google.com/bigquery)
- **Transformations:** [dbt](https://www.getdbt.com/)
- **Visualization:** [Google Data Studio / Looker Studio](https://lookerstudio.google.com/)

# Dataset
The data comes from access of a [public REST API](https://earthquake.usgs.gov/fdsnws/event/1/) from the [USGS](https://www.usgs.gov/) in collaboration with the [International Federation of Digital Seismograph Networks](http://www.fdsn.org/webservices/FDSN-WS-Specifications-1.0.pdf) (FDSN). The data is accessed through the query API endpoint, which has 3 parameters:
- format={geojson}
- starttime={yyyy-mm-dd}
- endtime={yyyy-mm-dd}

https://earthquake.usgs.gov/fdsnws/event/1/query?format={format}&starttime={yyyy-mm-dd}&endtime={yyyy-mm-dd}

The [geojson](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php) data format was used in this project.

- Example of json response:
![USGS API json Example](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/usgs_api_json.jpg)

# Data Transformation
- The [REST API json response](de_zoomcamp_2022_earthquake_capstone/sample_data/sample_earthquake_response.json) comes a nested json structure. 
- The seismic events that are of the most interest are in the "features" array of objects that needs to be iterated over and extracted into a Python array.
- This array was turned into a [Pandas dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
- Additional columns were created that converted the UNIX or POSIX time (ms) into datetime object.

- Example of Flattened data ![USGS API json Example](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/flattened_data.png)

# Data Modeling
- The data was denormalized and modeled using the One Big Table(OBT) method. 
- This allows for no joins of the data for the analyst and [typically faster query performance for data warehouses](https://www.fivetran.com/blog/star-schema-vs-obt). 
- Additional storage costs are typically a non-issue.

# Data Storage
- The both the raw json's and flattened parquet files are stored in a datalake in Google Cloud Storage bucket(GCS) 

![Data Lake](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/datalake.png)

# Airflow Orchestration 
The DAG does the following on a '@daily' schedule:
- Parameterizes the API endpoint to use the dates passed in by using the [Airflow provided template variables](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
- Uses the [Python requests library](https://requests.readthedocs.io/en/latest/) GET request and returns a API response to be stored in local memory with a variable.
- Saves the raw json file to local storage, uploads the blob into the datalake, then deletes the local json file.
- Uses Python to parse the nested JSON into list of dictionaries that gets transformed into a [Pandas dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
- Converts the DataFrame into a [parquet file](https://parquet.apache.org/docs/) that gets saved locally.
- Uploads the parquet into my [Google Cloud Storage](https://cloud.google.com/storage) datalake with the parameterized date as filename.
- Deletes the locally stored parquet file.
- Pushes the datalake parquet into a BigQuery Native table using the ['LOAD DATA INTO...'](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#appending_to_or_overwriting_a_table_with_parquet_data) command.
- Runs the dbt models using ['dbt_run'](https://docs.getdbt.com/reference/commands/run) command with the [BashOperator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html).

### Airflow DAG
![Airflow DAG](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/dag_graph.png)

# dbt Transformation
- dbt was used to take the `raw_earthquakes` data from BigQuery native table and deduplicate the data using a SQL window function.  
- The goal is to have only one record of seismic event using the seismic 'id' column.
- Requirements indicate that only the latest version of the event based on it's 'properties_updated_datetime' timestamp are needed.
- It is assumed as the seismic data is reprocessed, this archetecture design will allow analyst to examine the historical changes of a particular seismic event, yet still show the latest version of each event.

```
select 
	*,
	row_number() over(partition by id, properties_updated_datetime) as rn
from {{ source('raw','raw_earthquake') }}
where id is not null
```
- The `stg_earthquakes` was [materialized](https://docs.getdbt.com/docs/build/materializations) as [view](https://docs.getdbt.com/terms/view).
- The [cast function](https://docs.getdbt.com/sql-reference/cast) was used to rename columns and change data types.
- This view is the [source reference](https://docs.getdbt.com/reference/dbt-jinja-functions/source) for the `fact_earthquakes` table that is [partitioned](https://docs.getdbt.com/reference/resource-configs/bigquery-configs#partition-clause) by "properties_time_datetime" timestamp and materialized as an [incremental table](https://docs.getdbt.com/docs/build/incremental-models).

```
{{ config(
    materialized='incremental',
    partition_by={
      "field": "properties_updated_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}
```
### dbt Lineage Graph
![dbt Lineage Graph](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/dbt_lineage_graph.png)

# Visualization
- With the fact_table materialized in a partitioned BigQuery native table, the data can now be viewed in [Google Looker Studio (formerly Data Studio)](https://lookerstudio.google.com/)
- Looker Studio has 3 steps:
1. Pick a data source, including 100+ data connectors including BigQuery. 
2. Pick the columns needed, or create your own using logical functions.
3. Create the dashboard


![Step1](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/looker_step1.png)
![Step2](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/looker_step2.png)
![Step3](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/looker_step3.png)

# Future Work That Could Be Done
1. More experimentation with the Airflow configuration and VM instance. I would like to attempt a lightweight version using the [sequential executor](https://airflow.apache.org/docs/apache-airflow/stable/executor/sequential.html) instead of the [celery executor](https://airflow.apache.org/docs/apache-airflow/stable/executor/celery.html) and [SQLite over Postgres backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#choosing-database-backend).
1. I would like to try more automation around the devops implimentation of the entire pipeline.  I would like to see if I could automate more of the dockerfile to execute more steps of the initialization.
1. Explore making the airflow instance less brittle. Not using any local compute, but rather push more compute to external [cloud functions](https://cloud.google.com/functions).
1. Build out more reports, more testing, explore different methods surrounding dbt.
1. Data Science, Time Series Analysis

# Acknowledgements
Thanks to the instructors.
- [Alexey Grigorev](https://github.com/alexeygrigorev)
- [Sejal Vaidya](https://github.com/sejalv)
- [Victoria Perez Mola](https://github.com/Victoriapm)
- [Ankush Khanna](https://github.com/AnkushKhanna)

And my employer and teammates.
- [evolv Consulting](https://evolv.consulting/)
![evolv](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/evolv.jfif)

### LinkedIn
- [My Linkedin](https://www.linkedin.com/in/andynelson1982/)

<!-- # Helpful Resources
1. Data Engineering Resource Gathering
    [Joseph Machado - How to gather requirements for your data project](https://www.startdataengineering.com/post/n-questions-data-pipeline-req/)
1. [Fivetran Star Schema vs OBT](https://www.fivetran.com/blog/star-schema-vs-obt)
1. Airflow Resources
	1. Adylzhan Khashtamov, Kazakhstan DE
		1. https://github.com/adilkhash
		1. https://github.com/adilkhash/airflow-taskflow-api-examples
	1. Kenten Danas, Astronomer.io DE
		1. https://www.linkedin.com/in/kentendanas/
		1. https://www.youtube.com/watch?v=WOWh1lXKF-g&t=1232s
	1. Code2j
		1. https://www.youtube.com/@coder2j
		1. https://youtu.be/K9AnJ9_ZAXE
	1. Marc Lamberti, Airflow Instructor
		1. https://marclamberti.com/
	1. Data Pipelines with Apache Airflow - Bas Harenslak / Julian de Ruiter
		1. https://github.com/BasPH/data-pipelines-with-apache-airflow
1. Practicing with REST API
	1. https://gorest.co.in/ -->