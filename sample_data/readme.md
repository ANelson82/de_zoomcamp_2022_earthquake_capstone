# Example data I'm working with

The `sample_earthquake_response.json` shows the json repsonse I get from the API endpoint from the [USGS]('https://earthquake.usgs.gov/fdsnws/event/1/').

This nested json is denormalized using python in my airflow DAG.

The final state of the raw data is represented as `sample_earthquake_response_denormalized.csv`.