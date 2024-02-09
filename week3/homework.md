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
