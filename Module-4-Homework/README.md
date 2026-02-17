# Module 4 Homework:

## Question 1:

**Selected Answer:** `int_trips_unioned` only

### Explanation:
The `dbt run --select` command allows specific selection of nodes in the DAG.
- `+model`: Selects the model and all its parents (upstream).
- `model+`: Selects the model and all its children (downstream).
- `model`: Selects **only** the model specified.

Since the command `dbt run --select int_trips_unioned` has no `+` signs, dbt will isolate and run only that specific model, ignoring `stg_green_tripdata`, `stg_yellow_tripdata`, and any downstream models like `fct_trips`.


## Question 2:

**Answer:** **dbt will fail the test, returning a non-zero exit code**

### Explanation

The `accepted_values` test checks if all column values are in the allowed list.

```sql
SELECT *
FROM fct_trips
WHERE payment_type NOT IN (1, 2, 3, 4, 5)
```

When `payment_type = 6` appears, the test returns those rows and **fails**.


## Question 3:

**Query:**
```sql
SELECT COUNT(*) AS total_records
FROM fct_monthly_zone_revenue;
```

**Answer:** **12,998**

## Question 4:

**Query:**
```sql
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM fct_monthly_zone_revenue
WHERE 
    service_type = 'Green'
    AND revenue_year = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

**Answer:** **East Harlem North**
his query aggregates total revenue for Green taxis in 2020 by zone and returns the zone with highest revenue.


- `service_type = 'Green'` → Green taxis only
- `revenue_year = 2020` → Year 2020 only

SUM(revenue_monthly_total_amount)` sums all months

**Result:** East Harlem North had the highest total revenue for Green taxis in 2020.

## Question 5: Trip Count

**Query:**
```sql
SELECT 
    SUM(total_monthly_trips) AS total_trips
FROM fct_monthly_zone_revenue
WHERE 
    service_type = 'Green'
    AND revenue_year = 2019
    AND revenue_month = 10;
```

**Answer:** **384,624**

### Explanation

This query sums the total trips for Green taxis in October 2019.


- `service_type = 'Green'` → Green taxis only
- `revenue_year = 2019` → Year 2019
- `revenue_month = 10` → October

`SUM(total_monthly_trips)` sums trips from all zones in Oct 2019

**Result:** 384,624 total trips


## Question 6: FHV Staging Model

**Task:** Create staging model for FHV (For-Hire Vehicle) data for 2019.

**Requirements:**
1. Filter records where `dispatching_base_num IS NULL`
2. Rename columns to snake_case
3. Count final records

**Answer:** ✅ **43,244,693**

### Implementation

#### 1. Define Source

**File:** `models/staging/sources.yml`

```yaml
version: 2

sources:
  - name: staging
    database: your_project_id
    schema: taxi_data
    tables:
      - name: external_fhv_tripdata
        description: For-Hire Vehicle trip records for 2019
```

#### 2. Create Staging Model

**File:** `models/staging/stg_fhv_tripdata.sql`

```sql
{{
    config(
        materialized='view',
        tags=['staging', 'fhv']
    )
}}

WITH source AS (
    SELECT * 
    FROM {{ source('staging', 'external_fhv_tripdata') }}
),

filtered AS (
    SELECT *
    FROM source
    WHERE dispatching_base_num IS NOT NULL
),

renamed AS (
    SELECT
        dispatching_base_num,
        affiliated_base_number,
        
        {{ dbt_utils.generate_surrogate_key([
            'dispatching_base_num',
            'pickup_datetime'
        ]) }} AS trip_id,
        
        CAST(PUlocationID AS INTEGER) AS pickup_location_id,
        CAST(DOlocationID AS INTEGER) AS dropoff_location_id,
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
        CAST(SR_Flag AS INTEGER) AS shared_ride_flag
    FROM filtered
)

SELECT * FROM renamed

{% if var('is_test_run', default=true) %}
    LIMIT 100
{% endif %}
```

#### 3. Add Documentation

**File:** `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_fhv_tripdata
    description: Staging model for FHV trip data
    columns:
      - name: trip_id
        tests:
          - unique
          - not_null
      - name: dispatching_base_num
        tests:
          - not_null
      - name: pickup_location_id
        tests:
          - not_null
      - name: pickup_datetime
        tests:
          - not_null
```

#### 4. Install Packages

**File:** `packages.yml`

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

**Command:**
```bash
dbt deps
```

#### 5. Run Model

**Development:**
```bash
dbt run --select stg_fhv_tripdata --target dev
```

**Production:**
```bash
dbt run --select stg_fhv_tripdata --vars '{"is_test_run": false}' --target prod
```

#### 6. Count Records

**Query:**
```sql
SELECT COUNT(*) AS total_records
FROM stg_fhv_tripdata;
```

**Result:** 43,244,693 records

### Explanation

**Transformations applied:**
1. **Filter:** Removes rows with `dispatching_base_num IS NULL`
2. **Rename:** Standardizes column names to snake_case
3. **Cast:** Converts data types explicitly
4. **Surrogate Key:** Generates unique `trip_id` using dbt_utils

**CTE Structure:**
- `source` → Reads raw data
- `filtered` → Applies quality filter
- `renamed` → Standardizes columns

**Why 43,244,693:**
- Original data: ~43.3M records
- After filtering NULL: 43,244,693 records
- ~100k records removed due to missing dispatching_base_num