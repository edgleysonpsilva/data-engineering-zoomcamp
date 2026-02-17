{{
    config(
        materialized='view',
        tags=['staging', 'fhv', 'for_hire_vehicle']
    )
}}

{#
    Staging model for For-Hire Vehicle (FHV) trip data
    
    Source: NYC TLC FHV trip records for 2019
    
    Transformations:
    - Filter NULL dispatching_base_num (main data quality issue)
    - Rename columns to project standards (snake_case)
    - Cast location IDs and flags to INTEGER
    - Cast timestamps to TIMESTAMP type
    - Generate surrogate key from base number + pickup time
    
    Expected row count: ~43.2M records after filtering
#}

WITH source AS (
    
    -- Pull raw data from external source table
    SELECT * 
    FROM {{ source('staging', 'external_fhv_tripdata') }}

),

filtered AS (
    
    -- Apply primary data quality filter
    -- Remove records without valid dispatching base number
    SELECT *
    FROM source
    WHERE dispatching_base_num IS NOT NULL

),

renamed AS (
    
    SELECT
        -- ============================================================
        -- Identifiers
        -- ============================================================
        
        -- Primary business key (not unique alone)
        dispatching_base_num,
        
        -- May be NULL for independent operators
        affiliated_base_number,
        
        -- Generate unique surrogate key
        -- Combines base number + pickup timestamp
        {{ dbt_utils.generate_surrogate_key([
            'dispatching_base_num',
            'pickup_datetime'
        ]) }} AS trip_id,
        
        
        -- ============================================================
        -- Location IDs (cast for consistency across models)
        -- ============================================================
        
        CAST(PUlocationID AS INTEGER) AS pickup_location_id,
        CAST(DOlocationID AS INTEGER) AS dropoff_location_id,
        
        
        -- ============================================================
        -- Timestamps (explicit casting for data quality)
        -- ============================================================
        
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
        
        
        -- ============================================================
        -- Trip Information
        -- ============================================================
        
        -- Shared ride indicator
        -- NULL = Unknown, 0 = Not shared, 1 = Shared
        CAST(SR_Flag AS INTEGER) AS shared_ride_flag

    FROM filtered

)

-- Final select with optional test limit
SELECT * FROM renamed

-- Development testing limiter
-- Set is_test_run=false for production builds
{% if var('is_test_run', default=true) %}
    LIMIT 100
{% endif %}


{#
Usage:

Development (100 rows):
  dbt run --select stg_fhv_tripdata

Production (full data):
  dbt run --select stg_fhv_tripdata --vars '{"is_test_run": false}'

With dependencies:
  dbt build --select +stg_fhv_tripdata --vars '{"is_test_run": false}'
  
Tests:
  dbt test --select stg_fhv_tripdata
#}
