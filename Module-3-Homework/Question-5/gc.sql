CREATE OR REPLACE TABLE `ny_taxi.yellow_tripdata_2024_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `ny_taxi.external_yellow_tripdata_2024`;