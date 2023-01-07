# Earthquake Data Engineering Capstone Project

This project automatically ingests, stores, transforms the latest seismic activity data from the USGS (United States Geologic Society) for later analysis.

This GitHub repository fulfills the final capstone project for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks.Club](https://datatalks.club).

![Data Pipeline Architecture](https://github.com/ANelson82/de_zoomcamp_2022_earthquake_capstone/blob/main/images/architecture_earthquake.excalidraw.png)

# Presentation
#todo
[Youtube Presentation](http://youtube.com)

# Technical Challenge
Analysts are studying the frequency, intensity, and spatial occurrence of seismic activity.  This data needs to be automatically stored and processed in a way that analysts can quickly analyze and build out reports and dashboards.  The technical implementation needs to be:
1. Affordable 
1. Reliable
1. Scalable
1. Approachable (Easy to use)
1. Automated 
1. Flexible (to ingest additional data)
1. Accommodating (additional future analysis and machine learning)

# Technology Utilized
- **Infrastructure as code (IaC):** [Terraform](https://github.com/hashicorp/terraform)
- **Workflow orchestration:** [Airflow](https://airflow.apache.org/)
- **Containerization:** [Docker](https://www.docker.com/)
- **Data Lake:** [Google Cloud Storage (GCS)](https://cloud.google.com/storage)
- **Data Warehouse:** [BigQuery](https://cloud.google.com/bigquery)
- **Transformations:** [dbt](https://www.getdbt.com/)
- **Visualization:** [Google Data Studio / Looker Studio](https://lookerstudio.google.com/)

# Dataset
The data comes from access of a [public REST API](https://earthquake.usgs.gov/fdsnws/event/1/) from the [USGS](https://www.usgs.gov/) in collaboration with the [International Federation of Digital Seismograph Networks](http://www.fdsn.org/webservices/FDSN-WS-Specifications-1.0.pdf) (FDSN). The data is accessed through the query API endpoint, which has 3 parameters, data-format, startdate, enddate:

https://earthquake.usgs.gov/fdsnws/event/1/query?format={format}&starttime={yyyy-mm-dd}&endtime={yyyy-mm-dd}

For this project I used the [geojson](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php) format.

# Data Transformation
The [REST API json response](de_zoomcamp_2022_earthquake_capstone/sample_data/sample_earthquake_response.json) comes a nested json structure.  The seismic events that are of the most interest are in the "features" array that needs to be iterated over and extracted into a Python array.  This array was turned into a [Pandas dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).  Additional columns were created that converted the UNIX or POSIX time (ms) into datetime object.

# Data Modeling
The data was denormalized and modeled using the One Big Table(OBT) method.  This allows for no joins of the data for the analyst and [typically faster query performance for data warehouses](https://www.fivetran.com/blog/star-schema-vs-obt). Additional storage costs are typically a non-issue.

# Data Storage
- The data is stored initially in a raw stage as a native BigQuery table.
- The raw parquet files are 
- The data is transformed by dbt and materialized as a native BigQuery table that is 
# Airflow Orchestration 
The DAG does the following on a '@daily' schedule:
- Parameterizes the API endpoint to use the dates passed in by using the [Airflow provided template variables](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
- uses the [Python requests library](https://requests.readthedocs.io/en/latest/) store the API data in local memory into a variable
- uses Python to parse the nested JSON into list of dictionaries that gets transformed into a DataFrame
- converts the DataFrame into a [parquet file](https://parquet.apache.org/docs/) that gets saved locally
- uploads the parquet into my [Google Cloud Storage](https://cloud.google.com/storage) datalake with the parameterized date as filename
- #todo finish dag steps
- #todo add dag screenshot

# dbt Transformation
#todo dbt
# Dashboard
#todo Dashboard link and photo

# Future Work
1. More experimentation with the Airflow configuration and VM instance. I would like to attempt a lightweight version using the [sequential executor](https://airflow.apache.org/docs/apache-airflow/stable/executor/sequential.html) instead of the [celery executor](https://airflow.apache.org/docs/apache-airflow/stable/executor/celery.html) and [SQLite over Postgres backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#choosing-database-backend).
1. I would like to try more automation around the devops implimentation of the entire pipeline.  I would like to see if I could automate more of the dockerfile to execute more steps of the initialization.
1. Explore making the airflow instance less brittle. Not using any local compute, but rather push more compute to [cloud functions](https://cloud.google.com/functions)
1. Build out more reports, more testing, explore different methods surrounding dbt

# Acknowledgements
Thanks to the instructors.
- [Alexey Grigorev](https://github.com/alexeygrigorev)
- [Sejal Vaidya](https://github.com/sejalv)
- [Victoria Perez Mola](https://github.com/Victoriapm)
- [Ankush Khanna](https://github.com/AnkushKhanna)
- The other students

And my employer and teammates.
- [evolv Consulting](https://evolv.consulting/)


# Helpful Resources
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
1. Practicing with REST API
	1. https://gorest.co.in/