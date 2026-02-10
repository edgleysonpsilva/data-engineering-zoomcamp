## Question 2:

**Answer:** 0 MB for the External Table and 155.12 MB for the Materialized Table

### Steps to Reproduce:
I ran the following queries in BigQuery and checked the estimated bytes processed (top right corner of the UI) before execution.

1. **External Table:**
   ```sql
   SELECT COUNT(DISTINCT PULocationID) FROM `ny_taxi.external_yellow_tripdata_2024`;

2. **Materialize table:**

    ```sql
    SELECT COUNT(DISTINCT PULocationID) FROM `ny_taxi.yellow_tripdata_2024_non_partitioned`;
