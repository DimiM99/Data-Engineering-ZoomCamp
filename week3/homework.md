## Setup

### loadin the data
the data was loaded and pushed to gcs bucket using mage pipeline

### working with the data

#### create the external table with reference to the data in gcs
```sql
CREATE OR REPLACE EXTERNAL TABLE `peerless-sensor-411315.nyc_green_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-peerless-sensor-411315/nyc_green_taxi_2022_data.parquet']
);
```

#### create BQ table with the data from the external table
```sql
CREATE OR REPLACE TABLE `peerless-sensor-411315.nyc_green_taxi.green_tripdata_full`
AS SELECT * FROM `peerless-sensor-411315.nyc_green_taxi.external_green_tripdata`;
```

## Questions

#### Question 1
--> 840,402

#### query for question 2
```sql
-- distinct number of PULocationIDs in the dataset external table
SELECT DISTINCT(PULocationID)
FROM `peerless-sensor-411315.nyc_green_taxi.external_green_tripdata`;
--> estimated to process 0 B when run

-- distinct number of PULocationIDs in the dataset BQ table
SELECT DISTINCT(PULocationID)
FROM `peerless-sensor-411315.nyc_green_taxi.green_tripdata_full`;
--> estimated to process 6.41 MB when run
```

#### query for question 3
```sql
SELECT COUNT(1) FROM `peerless-sensor-411315.nyc_green_taxi.green_tripdata_full`
WHERE fare_amount = 0;
--> 1622
```

#### query for question 4
```sql
CREATE TABLE `peerless-sensor-411315.nyc_green_taxi.green_taxi_p_pudate_c_pu_loc`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `peerless-sensor-411315.nyc_green_taxi.green_tripdata_full`;
--> Partition by lpep_pickup_datetime Cluster on PUlocationID
```

#### queries for question 5
```sql
SELECT DISTINCT PULocationID
FROM `peerless-sensor-411315.nyc_green_taxi.green_taxi_p_pudate_c_pu_loc`
WHERE lpep_pickup_datetime >= '2022-06-01 00:00:00' 
AND lpep_pickup_datetime <= '2022-06-30 23:59:59';
--> estimated to process 1.12 MB when run
```
```sql
SELECT DISTINCT PULocationID
FROM `peerless-sensor-411315.nyc_green_taxi.green_tripdata_full`
WHERE lpep_pickup_datetime >= '2022-06-01 00:00:00' 
AND lpep_pickup_datetime <= '2022-06-30 23:59:59';
--> estimated to process 12.82 MB when run
```

#### question 6
Where is the data stored in the External Table you created?\
--> GCP Bucket

#### question 7
It is best practice in Big Query to always cluster your data:\
depends on your data so\
--> false

#### question 8
SELECT count(*) is estimated to process 0 B when run on the BQ table.\
I assume this query was run already run, for populating metadata purposes, and the result was cached.