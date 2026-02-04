
# Data-warehouse

In this section we focus on writing queries within BigQuery itself. We first create an external table by pointing to the location in our Cloud Storage (GCS) for BigQuery to pull data from the relevant files (parquet in this case).

We then do a few further queries, such as creating a non-partitioned native table, creating a partitioned equivalent, and also seeing how certain queries differ in estimated compute when for example querying one Vs two columns, or querying the same data from a partitioned Vs non-partitioned table.

The queries themselves can be found below, with the comments above giving a brief description of what that particular query is doing or looking at.


## Queries

```sql
--Create external table for 2024 trips (up to July)
CREATE OR REPLACE EXTERNAL TABLE `abiding-splicer-462011-a4.ny_taxi.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://abiding-splicer-462011-a4-terra-bucket/ny_taxi/yellow/yellow_tripdata_2024-*.parquet']
);

--Count the number of records. See it's 0B
SELECT COUNT(*) FROM `ny_taxi.external_yellow_tripdata_2024`;

--Create native table for same 2024 data
CREATE OR REPLACE TABLE `ny_taxi.yellow_tripdata_2024_non_partitioned` AS
SELECT * FROM `ny_taxi.external_yellow_tripdata_2024`;

--Run same count query, and again see it's 0B
SELECT COUNT(*) FROM `ny_taxi.yellow_tripdata_2024_non_partitioned`;

--One column 155MB, two columns 310MB
SELECT PULocationID from `ny_taxi.yellow_tripdata_2024_non_partitioned`;
SELECT PULocationID, DOLocationID from `ny_taxi.yellow_tripdata_2024_non_partitioned`;

--How many trips had fare_amount = 0?
SELECT count(1) FROM `ny_taxi.external_yellow_tripdata_2024`
WHERE fare_amount = 0;

--Now create a partitioned data based off pickup date
CREATE OR REPLACE TABLE `ny_taxi.yellow_tripdata_2024_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `ny_taxi.external_yellow_tripdata_2024`;

--Distinct VendorIDs between certain dates, 310MB
SELECT DISTINCT(VendorID)
FROM `ny_taxi.yellow_tripdata_2024_non_partitioned`
WHERE DATE (tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-16';

--Distinct VendorIDs between same dates against partitioned table, 28MB
SELECT DISTINCT(VendorID)
FROM `ny_taxi.yellow_tripdata_2024_partitioned`
WHERE DATE (tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-16';
```