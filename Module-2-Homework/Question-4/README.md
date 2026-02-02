### Question 4

**Answer:** 1,734,051

I created a specific flow `06_postgres_green_taxi_scheduled.yaml` to handle the Green Taxi schema and executed a Backfill for `2020-01-01` to `2020-12-31`.

Query used:
```sql
SELECT COUNT(*) FROM green_tripdata;
```