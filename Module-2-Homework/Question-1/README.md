### Question 1: File Size

**Answer:** 128.3 MiB

#### Execution Steps
1. **Infrastructure:** Deployed Kestra and Postgres locally using Docker Compose.
2. **Flow Creation:** Implemented the `05_postgres_taxi_scheduled` flow to handle CSV extraction and database loading.
3. **Backfill Execution:**
   - Navigate to the **Triggers** tab in Kestra UI.
   - Selected the `yellow_schedule` trigger.
   - Executed a **Backfill** with the following parameters:
     - **Start Date:** `2020-12-01`
     - **End Date:** `2020-12-31`
     - **Taxi Type:** `yellow`
4. **Result Verification:**
   - Inspected the execution logs.
   - Checked the **Outputs** tab for the `extract` task.
   - The output file `yellow_tripdata_2020-12.csv` was downloaded with a size of **128.3 MiB**.