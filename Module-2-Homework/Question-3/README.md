### Question 3

**Answer:** 24,648,499

I updated the flow `05_postgres_taxi_scheduled.yaml` to fix the schema mismatch and executed a Backfill for the period `2020-01-01` to `2020-12-31`. Then, I queried the database:

```sql
SELECT COUNT(*) FROM yellow_tripdata;
```