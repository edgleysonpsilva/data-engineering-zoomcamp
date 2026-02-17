-- ============================================================================
-- Module 4 Homework: SQL Queries
-- Analytics Engineering with dbt
-- ============================================================================

-- All queries follow dbt best practices:
-- - Use ref() for dbt models
-- - Clear, readable SQL
-- - Proper aggregations
-- - Documented with comments

-- ============================================================================
-- Question 3: Count records in fct_monthly_zone_revenue
-- ============================================================================

-- Basic count (expected: 12,998)
SELECT 
    COUNT(*) AS total_records
FROM {{ ref('fct_monthly_zone_revenue') }};


-- Data profiling for context
SELECT 
    COUNT(*) AS total_records,
    MIN(revenue_year) AS earliest_year,
    MAX(revenue_year) AS latest_year,
    COUNT(DISTINCT pickup_zone) AS unique_zones,
    COUNT(DISTINCT service_type) AS service_types,
    SUM(total_monthly_trips) AS all_trips,
    ROUND(SUM(revenue_monthly_total_amount), 2) AS all_revenue
FROM {{ ref('fct_monthly_zone_revenue') }};


-- Grain verification (should return 0 rows)
SELECT 
    pickup_zone,
    revenue_year,
    revenue_month,
    service_type,
    COUNT(*) as occurrences
FROM {{ ref('fct_monthly_zone_revenue') }}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;


-- ============================================================================
-- Question 4: Best performing zone for Green taxis in 2020
-- ============================================================================

-- Main query (expected: East Harlem North)
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue_2020,
    SUM(total_monthly_trips) AS total_trips,
    ROUND(
        SUM(revenue_monthly_total_amount) / 
        NULLIF(SUM(total_monthly_trips), 0), 
        2
    ) AS avg_revenue_per_trip
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    service_type = 'Green'
    AND revenue_year = 2020
GROUP BY pickup_zone
ORDER BY total_revenue_2020 DESC
LIMIT 10;


-- Year-over-year comparison
WITH yearly_revenue AS (
    SELECT 
        pickup_zone,
        revenue_year,
        SUM(revenue_monthly_total_amount) AS annual_revenue,
        SUM(total_monthly_trips) AS annual_trips
    FROM {{ ref('fct_monthly_zone_revenue') }}
    WHERE service_type = 'Green'
    GROUP BY 1, 2
)

SELECT 
    pickup_zone,
    MAX(CASE WHEN revenue_year = 2019 THEN annual_revenue END) AS revenue_2019,
    MAX(CASE WHEN revenue_year = 2020 THEN annual_revenue END) AS revenue_2020,
    MAX(CASE WHEN revenue_year = 2019 THEN annual_trips END) AS trips_2019,
    MAX(CASE WHEN revenue_year = 2020 THEN annual_trips END) AS trips_2020,
    ROUND(
        (MAX(CASE WHEN revenue_year = 2020 THEN annual_revenue END) - 
         MAX(CASE WHEN revenue_year = 2019 THEN annual_revenue END)) / 
        NULLIF(MAX(CASE WHEN revenue_year = 2019 THEN annual_revenue END), 0) * 100,
        2
    ) AS revenue_pct_change
FROM yearly_revenue
GROUP BY pickup_zone
HAVING MAX(CASE WHEN revenue_year = 2020 THEN annual_revenue END) IS NOT NULL
ORDER BY revenue_2020 DESC
LIMIT 10;


-- ============================================================================
-- Question 5: Green taxi trip counts for October 2019
-- ============================================================================

-- Main query (expected: 384,624)
SELECT 
    SUM(total_monthly_trips) AS october_2019_trips
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    service_type = 'Green'
    AND revenue_year = 2019
    AND revenue_month = 10;


-- Monthly trend for 2019
SELECT 
    revenue_month,
    SUM(total_monthly_trips) AS monthly_trips,
    ROUND(SUM(revenue_monthly_total_amount), 2) AS monthly_revenue,
    ROUND(
        SUM(revenue_monthly_total_amount) / 
        NULLIF(SUM(total_monthly_trips), 0),
        2
    ) AS avg_fare
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    service_type = 'Green'
    AND revenue_year = 2019
GROUP BY revenue_month
ORDER BY revenue_month;


-- Top zones in October 2019
SELECT 
    pickup_zone,
    SUM(total_monthly_trips) AS trips,
    ROUND(SUM(revenue_monthly_total_amount), 2) AS revenue
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    service_type = 'Green'
    AND revenue_year = 2019
    AND revenue_month = 10
GROUP BY pickup_zone
ORDER BY trips DESC
LIMIT 20;


-- ============================================================================
-- Question 6: Count records in stg_fhv_tripdata
-- ============================================================================

-- Basic count (expected: 43,244,693)
SELECT 
    COUNT(*) AS total_records
FROM {{ ref('stg_fhv_tripdata') }};


-- Comprehensive data profiling
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT trip_id) AS unique_trip_ids,
    COUNT(DISTINCT dispatching_base_num) AS unique_bases,
    COUNT(DISTINCT pickup_location_id) AS unique_pickup_zones,
    COUNT(DISTINCT dropoff_location_id) AS unique_dropoff_zones,
    MIN(pickup_datetime) AS earliest_trip,
    MAX(pickup_datetime) AS latest_trip,
    COUNTIF(shared_ride_flag = 1) AS shared_rides,
    COUNTIF(shared_ride_flag = 0) AS non_shared_rides,
    COUNTIF(shared_ride_flag IS NULL) AS unknown_shared,
    COUNTIF(affiliated_base_number IS NOT NULL) AS with_affiliation,
    COUNTIF(affiliated_base_number IS NULL) AS without_affiliation
FROM {{ ref('stg_fhv_tripdata') }};


-- Monthly distribution
SELECT 
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    COUNT(*) AS trips,
    COUNT(DISTINCT dispatching_base_num) AS active_bases
FROM {{ ref('stg_fhv_tripdata') }}
GROUP BY month
ORDER BY month;


-- Top dispatching bases
SELECT 
    dispatching_base_num,
    COUNT(*) AS total_trips,
    COUNTIF(shared_ride_flag = 1) AS shared_rides,
    ROUND(
        COUNTIF(shared_ride_flag = 1) * 100.0 / COUNT(*),
        2
    ) AS shared_ride_pct
FROM {{ ref('stg_fhv_tripdata') }}
GROUP BY dispatching_base_num
ORDER BY total_trips DESC
LIMIT 20;


-- ============================================================================
-- Bonus Queries: Cross-analysis
-- ============================================================================

-- Compare Green vs Yellow taxi performance
SELECT 
    service_type,
    revenue_year,
    SUM(total_monthly_trips) AS total_trips,
    ROUND(SUM(revenue_monthly_total_amount), 2) AS total_revenue,
    COUNT(DISTINCT pickup_zone) AS zones_served,
    ROUND(
        SUM(revenue_monthly_total_amount) / 
        NULLIF(SUM(total_monthly_trips), 0),
        2
    ) AS avg_fare
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE revenue_year IN (2019, 2020)
GROUP BY service_type, revenue_year
ORDER BY revenue_year, service_type;


-- Seasonal patterns
SELECT 
    revenue_month,
    service_type,
    SUM(total_monthly_trips) AS trips,
    ROUND(SUM(revenue_monthly_total_amount), 2) AS revenue
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE revenue_year = 2019
GROUP BY revenue_month, service_type
ORDER BY revenue_month, service_type;


-- Zone performance across both services
SELECT 
    pickup_zone,
    SUM(CASE WHEN service_type = 'Green' THEN total_monthly_trips END) AS green_trips,
    SUM(CASE WHEN service_type = 'Yellow' THEN total_monthly_trips END) AS yellow_trips,
    SUM(total_monthly_trips) AS total_trips,
    ROUND(SUM(revenue_monthly_total_amount), 2) AS total_revenue
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE revenue_year IN (2019, 2020)
GROUP BY pickup_zone
HAVING SUM(total_monthly_trips) > 1000
ORDER BY total_trips DESC
LIMIT 20;


-- ============================================================================
-- Data Quality Checks
-- ============================================================================

-- Check for NULL values in critical fields
SELECT 
    'fct_monthly_zone_revenue' AS model,
    COUNTIF(pickup_zone IS NULL) AS null_zones,
    COUNTIF(revenue_year IS NULL) AS null_years,
    COUNTIF(revenue_month IS NULL) AS null_months,
    COUNTIF(service_type IS NULL) AS null_service_types,
    COUNTIF(total_monthly_trips IS NULL) AS null_trips,
    COUNTIF(revenue_monthly_total_amount IS NULL) AS null_revenue
FROM {{ ref('fct_monthly_zone_revenue') }};


-- Check for negative values
SELECT 
    pickup_zone,
    revenue_year,
    revenue_month,
    total_monthly_trips,
    revenue_monthly_total_amount
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    total_monthly_trips < 0 
    OR revenue_monthly_total_amount < 0
LIMIT 100;


-- Check for unreasonable values
SELECT 
    pickup_zone,
    revenue_year,
    revenue_month,
    total_monthly_trips,
    revenue_monthly_total_amount,
    ROUND(
        revenue_monthly_total_amount / 
        NULLIF(total_monthly_trips, 0),
        2
    ) AS avg_fare
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    -- Flag suspicious average fares
    (revenue_monthly_total_amount / NULLIF(total_monthly_trips, 0)) > 500
    OR (revenue_monthly_total_amount / NULLIF(total_monthly_trips, 0)) < 0
LIMIT 100;


-- ============================================================================
-- End of Homework Queries
-- ============================================================================

/*
Usage Notes:

1. Run these queries in your SQL editor or via dbt compile
2. Use {{ ref() }} for referencing dbt models
3. All queries are production-ready and follow best practices
4. Expected results are documented in comments
5. Data quality checks help validate your models

For dbt Cloud:
  - Run queries in the IDE SQL editor
  - Replace {{ ref() }} with full table names

For dbt Core:
  - Run via dbt compile then execute in your warehouse
  - Or use dbt run-operation with macros

Happy querying! 🚀
*/
