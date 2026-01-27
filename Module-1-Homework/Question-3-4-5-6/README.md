# Question 3

```bash
import pandas as pd
from sqlalchemy import create_engine


engine = create_engine('postgresql+psycopg://postgres:postgres@localhost:5433/ny_taxi')

# read. parquet file
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
df = pd.read_parquet(url)

# send to postgres
df.to_sql(name='green_trips', con=engine, if_exists='replace)


query = """
SELECT count(*)
FROM green_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
"""

df_result = pd.read_sql(query, con=engine)
print(df_result)

``` 

Output:

```bash
   count
0   8007
```

# Question 4

```bash
#Question 4

query = """
SELECT
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_distance
FROM
    green_trips
WHERE
    trip_distance < 100  -- Excluindo erros de dados conforme enunciado
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 5;
"""

df_result = pd.read_sql(query, con=engine)
print(df_result)
```

Output:

```bash
   pickup_day  max_distance
0  2025-11-14         88.03
1  2025-11-20         73.84
2  2025-11-23         45.26
3  2025-11-22         40.16
4  2025-11-15         39.81
```

# Question 5

```bash
url_zones = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
df_zones = pd.read_csv(url_zones)
df_zones.to_sql(name='zones', con=engine, if_exists='replace')


# t trips
#z zone

query_q5 = """
SELECT
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_sum
FROM
    green_trips t
JOIN
    zones z ON t."PULocationID" = z."LocationID"
WHERE
    DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 5;
"""

df_q5 = pd.read_sql(query_q5, con=engine)
print(df_q5)
```
Output

```bash
                pickup_zone  total_sum
0         East Harlem North    9281.92
1         East Harlem South    6696.13
2              Central Park    2378.79
3  Washington Heights South    2139.05
4       Morningside Heights    2100.59
```


# Question 6

```bash
# zpu zone pick up
# zdo zone drop off

query_q6 = """
SELECT
    zdo."Zone" AS dropoff_zone,
    t.tip_amount
FROM
    green_trips t
JOIN
    zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN
    zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'East Harlem North'
    AND t.lpep_pickup_datetime >= '2025-11-01'
    AND t.lpep_pickup_datetime < '2025-12-01'
    AND zdo."Zone" IS NOT NULL
ORDER BY
    t.tip_amount DESC
LIMIT 5;
"""

df_q6 = pd.read_sql(query_q6, con=engine)
print(df_q6)
```

Output:

```bash
                    dropoff_zone  tip_amount
0                 Yorkville West       81.89
1              LaGuardia Airport       50.00
2              East Harlem North       45.00
3  Long Island City/Queens Plaza       34.25
```

# FINAL ANSWERS

```bash
Question 3: 8007

Question 4: 2025-11-14

Question 5: East Harlem North

Questão 6: Yorkville West
``