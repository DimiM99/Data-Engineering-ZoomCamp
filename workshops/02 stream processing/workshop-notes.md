### Set-up
#### python env setup
I will not be using pyenv or virtualenv for this workshop.\
I will be using conda to manage my python environment.

```bash
# create a new conda environment
cd w6-ws2-stream-processing-project
conda create --prefix ./.venv/w6-ws2-risingwave-env python=3.9
# activate the environment
conda activate ./.venv/w6-ws2-risingwave-env
# install the required packages
pip install -r requirements.txt 
```
additionally (to use psql): 
```bash
brew install postgresql@14
```

#### before proceeding
custom commands and env variables need to be fed each time into running shell session\
where any command is required to be run.
```bash
source commands.sh
```

#### starting the project
start the underlying services in the background
```bash
start-cluster
```

#### starting the stream
```bash
tream-kafka
```

#### connecting to the database (rw-sql)
```bash
psql -p 4566 -h localhost -d dev -U root 
```

#### create the table in risingwave (source)
```bash
psql -p 4566 -h localhost -d dev -U root -f risingwave-sql/table/trip_data.sql
# or 
rw-sql -f risingwave-sql/table/trip_data.sql
```    

### Working with stream porcessing
psql works like crap, so I will use the built-in sql console in PyCharm 

#### Ingested data Validation
##### making sure we're getting the recent data and joing it with the zones information
###### input
```sql
SELECT taxi_zone.Zone as pickup_zone, taxi_zone_1.Zone as dropoff_zone, tpep_pickup_datetime, tpep_dropoff_datetime
 FROM trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
 WHERE tpep_dropoff_datetime > now() - interval '1 minute';
```
###### output (fisrt 10 row)
```
|#  |pickup_zone                  |dropoff_zone              |tpep_pickup_datetime       |tpep_dropoff_datetime     |
|---|-----------------------------|--------------------------|---------------------------|--------------------------|
|1  |Times Sq/Theatre District    |East Harlem North         |2024-03-07 14:01:42.261000 |2024-03-07 14:18:15.261000|
|2  |Midtown East                 |LaGuardia Airport         |2024-03-07 13:59:52.073000 |2024-03-07 14:18:16.073000|
|3  |NV                           |NV                        |2024-03-07 14:05:49.406000 |2024-03-07 14:18:16.406000|
|4  |Upper West Side South        |Lincoln Square West       |2024-03-07 14:11:38.620000 |2024-03-07 14:18:16.620000|
|5  |JFK Airport                  |West Village              |2024-03-07 13:43:21.178000 |2024-03-07 14:18:17.178000|
|6  |UN/Turtle Bay South          |JFK Airport               |2024-03-07 13:54:25.615000 |2024-03-07 14:18:17.615000|
|7  |Midtown North                |Times Sq/Theatre District |2024-03-07 14:13:11.771000 |2024-03-07 14:18:17.771000|
|8  |Midtown Center               |Gramercy                  |2024-03-07 14:10:54.861000 |2024-03-07 14:18:17.861000|
|9  |Morningside Heights          |Midtown North             |2024-03-07 13:59:27.060000 |2024-03-07 14:18:19.060000|
|10 |Lincoln Square East          |Upper East Side North     |2024-03-07 14:07:59.099000 |2024-03-07 14:18:19.099000|
```

##### making it the mv out of the recent data
```sql
CREATE MATERIALIZED VIEW latest_1min_trip_data AS
       -- the rest is the same as in the query
```

##### testing the mv and querying out of it
###### input
```sql
SELECT * FROM latest_1min_trip_data order by tpep_dropoff_datetime DESC;
```
###### output (first 10 lines)
```
|#  |pickup_zone                 |dropoff_zone                  |tpep_pickup_datetime       |tpep_dropoff_datetime     |
|---|----------------------------|------------------------------|---------------------------|--------------------------|
|1  |West Chelsea/Hudson Yards   |UN/Turtle Bay South           |2024-03-07 14:14:41.347000 |2024-03-07 14:27:59.347000|
|2  |East Village                |East Chelsea                  |2024-03-07 14:08:49.250000 |2024-03-07 14:27:59.250000|
|3  |East Chelsea                |East Village                  |2024-03-07 14:13:14.198000 |2024-03-07 14:27:59.198000|
|4  |Gramercy                    |Midtown East                  |2024-03-07 14:19:08.180000 |2024-03-07 14:27:59.180000|
|5  |Greenwich Village North     |Greenwich Village South       |2024-03-07 14:22:41.111000 |2024-03-07 14:27:59.111000|
|6  |JFK Airport                 |Yorkville West                |2024-03-07 13:58:27.080000 |2024-03-07 14:27:59.080000|
|7  |Upper West Side South       |Gramercy                      |2024-03-07 14:12:12.072000 |2024-03-07 14:27:59.072000|
|8  |Clinton East                |Central Park                  |2024-03-07 14:23:56.065000 |2024-03-07 14:27:59.065000|
|9  |LaGuardia Airport           |Sutton Place/Turtle Bay North |2024-03-07 14:08:14.021000 |2024-03-07 14:27:59.021000|
|10 |Upper West Side South       |Hamilton Heights              |2024-03-07 14:13:04.950000 |2024-03-07 14:27:58.950000|
```

#### MV 1 Trips from Airport

##### check which zones have Airport in the name
###### input
```sql
SELECT * FROM taxi_zone WHERE Zone LIKE '%Airport';
```
###### output
```
|#  |location_id                  |borough          |zone                      |service_zone              |
|---|-----------------------------|-----------------|--------------------------|--------------------------|
|1  |1                            |EWR              |Newark Airport            |EWR                       |
|2  |132                          |Queens           |JFK Airport               |Airports                  |
|3  |138                          |Queens           |LaGuardia Airport         |Airports                  |
```

##### joint the trip and zone data to get trip data with trips form airport
###### input
```sql
SELECT
        *
    FROM
        trip_data
            JOIN taxi_zone
                 ON trip_data.PULocationID = taxi_zone.location_id
    WHERE taxi_zone.Zone LIKE '%Airport';
```
###### output (first 10 rows)
```
|#  |vendorid                     |tpep_pickup_datetime       |tpep_dropoff_datetime     |passenger_count |trip_distance|ratecodeid|store_and_fwd_flag|pulocationid|dolocationid|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee|location_id|borough|zone             |service_zone|
|---|-----------------------------|---------------------------|--------------------------|----------------|-------------|----------|------------------|------------|------------|------------|-----------|-----|-------|----------|------------|---------------------|------------|--------------------|-----------|-----------|-------|-----------------|------------|
|1  |2                            |2024-03-07 13:30:38.649000 |2024-03-07 13:44:08.649000|2               |6.49         |1         |N                 |138         |112         |1           |19.5       |0.5  |0.5    |6.62      |0           |0.3                  |28.67       |0                   |1.25       |138        |Queens |LaGuardia Airport|Airports    |
|2  |1                            |2024-03-07 13:17:21.907000 |2024-03-07 13:44:18.907000|1               |17.2         |1         |N                 |132         |256         |1           |46.5       |1.75 |0.5    |14.7      |0           |0.3                  |63.75       |0                   |1.25       |132        |Queens |JFK Airport      |Airports    |
|3  |2                            |2024-03-07 13:09:15.875000 |2024-03-07 13:44:19.875000|2               |11.18        |1         |N                 |132         |85          |2           |36         |0.5  |0.5    |0         |0           |0.3                  |38.55       |0                   |1.25       |132        |Queens |JFK Airport      |Airports    |
|4  |2                            |2024-03-07 13:16:32.802000 |2024-03-07 13:44:30.802000|3               |19.55        |1         |N                 |132         |33          |1           |52.5       |0.5  |0.5    |8.26      |0           |0.3                  |63.31       |0                   |1.25       |132        |Queens |JFK Airport      |Airports    |
|5  |1                            |2024-03-07 13:20:23.726000 |2024-03-07 13:44:32.726000|1               |9.8          |1         |Y                 |138         |225         |1           |30         |1.75 |0.5    |8.1       |0           |0.3                  |40.65       |0                   |1.25       |138        |Queens |LaGuardia Airport|Airports    |
|6  |2                            |2024-03-07 13:07:01.557000 |2024-03-07 13:44:39.557000|1               |18.21        |1         |N                 |132         |89          |1           |52         |0.5  |0.5    |21.32     |0           |0.3                  |75.87       |0                   |1.25       |132        |Queens |JFK Airport      |Airports    |
|7  |2                            |2024-03-07 13:09:43.332000 |2024-03-07 13:44:43.332000|2               |20.01        |1         |N                 |132         |80          |2           |53         |0.5  |0.5    |0         |0           |0.3                  |55.55       |0                   |1.25       |132        |Queens |JFK Airport      |Airports    |
|8  |2                            |2024-03-07 13:22:48.681000 |2024-03-07 13:44:46.681000|1               |10.83        |1         |N                 |132         |129         |2           |31         |0.5  |0.5    |0         |0           |0.3                  |33.55       |0                   |1.25       |132        |Queens |JFK Airport      |Airports    |
|9  |1                            |2024-03-07 12:57:34.407000 |2024-03-07 13:44:57.407000|1               |24.6         |1         |N                 |132         |241         |1           |68.5       |1.75 |0.5    |15.5      |6.55        |0.3                  |93.1        |0                   |1.25       |132        |Queens |JFK Airport      |Airports    |
|10 |2                            |2024-03-07 13:45:03.162000 |2024-03-07 13:45:10.162000|1               |0            |1         |N                 |132         |132         |2           |2.5        |0.5  |0.5    |0         |0           |0.3                  |3.8         |0                   |0          |132        |Queens |JFK Airport      |Airports    |
```

##### count total trips fot rach airport zone
###### input
```sql
SELECT
        count(*) AS cnt,
        taxi_zone.Zone
    FROM
        trip_data
            JOIN taxi_zone
                 ON trip_data.PULocationID = taxi_zone.location_id
    WHERE taxi_zone.Zone LIKE '%Airport'
    GROUP BY taxi_zone.Zone;
```
###### output
```
|#  |cnt       |zone             |
|---|----------|-----------------|
|1  |54        |Newark Airport   |
|2  |2259      |LaGuardia Airport|
|3  |5073      |JFK Airport      |
```

##### create a mv for this for queries 
```sql
CREATE MATERIALIZED VIEW total_airport_pickups AS
    SELECT
        count(*) AS cnt,
        taxi_zone.Zone
    FROM
        trip_data
            JOIN taxi_zone
                ON trip_data.PULocationID = taxi_zone.location_id
    WHERE taxi_zone.Zone LIKE '%Airport'
    GROUP BY taxi_zone.Zone;
```


##### a plan for mv for counting total trips fot rach airport zone
###### input
```sql
EXPLAIN CREATE MATERIALIZED VIEW total_airport_pickups AS
    SELECT
        count(*) AS cnt,
        taxi_zone.Zone
    FROM
        trip_data
            JOIN taxi_zone
                 ON trip_data.PULocationID = taxi_zone.location_id
    WHERE taxi_zone.Zone LIKE '%Airport'
    GROUP BY taxi_zone.Zone;
```
###### output
```
StreamMaterialize { columns: [cnt, zone], stream_key: [zone], pk_columns: [zone], pk_conflict: NoCheck }"
└─StreamProject { exprs: [count, taxi_zone.zone] }"
  └─StreamHashAgg { group_key: [taxi_zone.zone], aggs: [count] }"
   └─StreamExchange { dist: HashShard(taxi_zone.zone) }
      └─StreamHashJoin { type: Inner, predicate: trip_data.pulocationid = $expr1 }"
       ├─StreamExchange { dist: HashShard(trip_data.pulocationid) }
       │ └─StreamTableScan { table: trip_data, columns: [pulocationid, _row_id] }"
       └─StreamExchange { dist: HashShard($expr1) }
          └─StreamProject { exprs: [taxi_zone.zone, taxi_zone.location_id::Int64 as $expr1, taxi_zone._row_id] }"
            └─StreamFilter { predicate: Like(taxi_zone.zone, '%Airport':Varchar) }"
              └─StreamTableScan { table: taxi_zone, columns: [location_id, zone, _row_id] }"
```

#### MV 2 Airport pickups from JFK Airport, 1 hour before the latest pickup

We can adapt the previous MV to create a more specific one.
We no longer need the `GROUP BY`, since we are only interested in 1 taxi zone, JFK Airport.
##### Pickups from 'JFK Airport'
```sql
CREATE MATERIALIZED VIEW airport_pu as
SELECT
    tpep_pickup_datetime,
    pulocationid
FROM
    trip_data
        JOIN taxi_zone
            ON trip_data.PULocationID = taxi_zone.location_id
WHERE
        taxi_zone.Borough = 'Queens'
  AND taxi_zone.Zone = 'JFK Airport';
```

##### keep track of the latest pickup
```sql
CREATE MATERIALIZED VIEW latest_jfk_pickup AS
    SELECT
        max(tpep_pickup_datetime) AS latest_pickup_time
    FROM
        trip_data
            JOIN taxi_zone
                ON trip_data.PULocationID = taxi_zone.location_id
    WHERE
        taxi_zone.Borough = 'Queens'
      AND taxi_zone.Zone = 'JFK Airport';
```

##### counts of the pickups from JFK Airport, 1 hour before the latest pickup
```sql
CREATE MATERIALIZED VIEW jfk_pickups_1hr_before AS
    SELECT
        count(*) AS cnt
    FROM
        airport_pu
            JOIN latest_jfk_pickup
                ON airport_pu.tpep_pickup_datetime > latest_jfk_pickup.latest_pickup_time - interval '1 hour'
            JOIN taxi_zone
                ON airport_pu.PULocationID = taxi_zone.location_id
    WHERE
        taxi_zone.Borough = 'Queens'
      AND taxi_zone.Zone = 'JFK Airport';
```

##### Plan for the thing
```
StreamMaterialize { columns: [cnt], stream_key: [], pk_columns: [], pk_conflict: NoCheck }"
└─StreamProject { exprs: [sum0(count)] }
  └─StreamSimpleAgg { aggs: [sum0(count), count] }"
   └─StreamExchange { dist: Single }
     └─StreamStatelessSimpleAgg { aggs: [count] }
        └─StreamProject { exprs: [airport_pu.trip_data._row_id, airport_pu.taxi_zone._row_id, airport_pu.pulocationid, taxi_zone._row_id] }"
          └─StreamDynamicFilter { predicate: (airport_pu.tpep_pickup_datetime > $expr2), output: [airport_pu.tpep_pickup_datetime, airport_pu.trip_data._row_id, airport_pu.taxi_zone._row_id, airport_pu.pulocationid, taxi_zone._row_id] }"
            ├─StreamHashJoin { type: Inner, predicate: airport_pu.pulocationid = $expr1 }"
            │ ├─StreamExchange { dist: HashShard(airport_pu.pulocationid) }
            │ │ └─StreamTableScan { table: airport_pu, columns: [tpep_pickup_datetime, pulocationid, trip_data._row_id, taxi_zone._row_id] }"
            │ └─StreamExchange { dist: HashShard($expr1) }
            │   └─StreamProject { exprs: [taxi_zone.location_id::Int64 as $expr1, taxi_zone._row_id] }"
            │     └─StreamFilter { predicate: (taxi_zone.borough = 'Queens':Varchar) AND (taxi_zone.zone = 'JFK Airport':Varchar) }
            │       └─StreamTableScan { table: taxi_zone, columns: [location_id, _row_id, borough, zone] }"
            └─StreamExchange { dist: Broadcast }
                └─StreamProject { exprs: [(latest_jfk_pickup.latest_pickup_time - '01:00:00':Interval) as $expr2] }
                    └─StreamTableScan { table: latest_jfk_pickup, columns: [latest_pickup_time] }"
```