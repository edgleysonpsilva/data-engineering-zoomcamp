import pandas as pd
from sqlalchemy import create_engine


engine = create_engine('postgresql://postgres:postgres@localhost:5432/ny_taxi')

# read. parquet file
file_name = 'green_tripdata_2025-11.parquet'
df = pd.read_parquet(file_name)

# send to postgres
df.to_sql(name='green_trips', con=engine, if_exists='replace')