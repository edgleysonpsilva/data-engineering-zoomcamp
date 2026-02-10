CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-2026-eps/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `ny_taxi.yellow_tripdata_2024_non_partitioned` AS
    SELECT * FROM `ny_taxi.external_yellow_tripdata_2024`;

 SELECT COUNT(*) FROM `ny_taxi.yellow_tripdata_2024_non_partitioned`;