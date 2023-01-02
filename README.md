# earthquake_2023

Time to type notes here.

# Airflow Steps
For my capstone I've successfully authored a DAG that does the following on a '@daily' schedule:
parameterized my API endpoint to use the dates passed in by Airflow using the provided macro templates
uses the python requests library store the API data in local memory into a variable
uses python to parse the nested JSON into List of dictionaries that gets transformed into a DataFrame
converts the DataFrame into a parquet file that gets saved locally
uploads the parquet into my GCS datalake with the parameterized date as filename
