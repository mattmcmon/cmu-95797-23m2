-- Creates bike_data table from file
create or replace table bike_data as select * from
read_csv_auto('./data/citibike-tripdata*.csv.gz',
union_by_name=True, filename=True, all_varchar=1, header=True);

-- Creates central_park_weather table from file
create or replace table central_park_weather as select * from
read_csv_auto('./data/central_park_weather*.csv',  
union_by_name=True, filename=True, all_varchar=1, header=True);

-- Creates fhv_bases table from file
create or replace table fhv_bases as select * from
read_csv_auto('./data/fhv_bases.csv',  
union_by_name=True, filename=True, all_varchar=1, header=True);

-- Creates fhv_tripdata table from file
create or replace table fhv_tripdata as select * from
read_parquet('./data/taxi/fhv_tripdata*.parquet',
union_by_name=True, filename=True);

-- Creates fhvhv_tripdata table from file
create or replace table fhvhv_tripdata as select * from
read_parquet('./data/taxi/fhvhv_tripdata*.parquet',
union_by_name=True, filename=True);

-- Creates green_tripdata table from file
create or replace table green_tripdata as select * from
read_parquet('./data/taxi/green_tripdata*.parquet',
union_by_name=True, filename=True);

-- Creates yellow_tripdata table from file
create or replace table yellow_tripdata as select * from
read_parquet('./data/taxi/yellow_tripdata*.parquet',
union_by_name=True, filename=True);
