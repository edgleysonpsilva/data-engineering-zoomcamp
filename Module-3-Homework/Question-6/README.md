## Question 6:

**Answer:** 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

### Steps to Reproduce:
I ran the following queries and observed the estimated bytes processed.

   ```sql
   SELECT DISTINCT VendorID
   FROM `ny_taxi.yellow_tripdata_2024_non_partitioned`
   WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';