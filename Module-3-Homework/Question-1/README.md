## Question 1: Counting Records

**Answer:** 20332093


I created a Python script `load_yellow_taxi_2024.py` to download the Yellow Taxi Parquet files for January to June 2024 and upload them to my Google Cloud Storage (GCS) bucket.

I created an external table in BigQuery that reads the data directly from the GCS bucket without moving it.
    ```sql
    CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_yellow_tripdata_2024`
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://zoomcamp-2026-eps/yellow_tripdata_2024-*.parquet']
    );
    ```
I materialized the data into a standard BigQuery table (non-partitioned) to compare performance later.
    ```sql
    CREATE OR REPLACE TABLE `ny_taxi.yellow_tripdata_2024_non_partitioned` AS
    SELECT * FROM `ny_taxi.external_yellow_tripdata_2024`;
    ```

Finally, I ran a simple count query to get the total number of records.
    ```sql
    SELECT COUNT(*) FROM `ny_taxi.yellow_tripdata_2024_non_partitioned`;
    ```