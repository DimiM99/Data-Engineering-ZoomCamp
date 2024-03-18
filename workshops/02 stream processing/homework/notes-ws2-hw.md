
### Question 1 and Question 2
```sql
CREATE OR REPLACE MATERIALIZED VIEW taxi_zone_trip_stats AS
    SELECT
        pickup.zone AS pickup_zone,
        dropoff.zone AS dropoff_zone,
        MD5(
            -- This is to ensure that the same pair of zones is always represented by the same identifier
            CASE WHEN pickup.zone < dropoff.zone THEN pickup.zone || '|' || dropoff.zone
            ELSE dropoff.zone || '|' || pickup.zone END
        ) AS identifier,
        count(*) AS trip_count,
        AVG(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
        MIN(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
        MAX(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS max_trip_time_minutes
    FROM
        trip_data td
    JOIN
        taxi_zone pickup ON td.PULocationID = pickup.location_id
    JOIN
        taxi_zone dropoff ON td.DOLocationID = dropoff.location_id
    GROUP BY
        pickup.zone, dropoff.zone, identifier;
```

```
|pickup_zone                    |dropoff_zone              |identifier (md5)                 |trip count    |avg                            |min                            |max                           |
|-------------------------------|--------------------------|---------------------------------|--------------|-------------------------------|------------------------------ |------------------------------|
|Yorkville East                 |Steinway                  |3cdd5246e2bda5f7370e5194d1bc1e71 |1             |1439.55                        |1439.55                        |1439.55                       |
|Stuy Town/Peter Cooper Village |Murray Hill-Queens        |cda93ec77d76df3f6f64f82dfe404557 |1             |1438.7333333333333333333333333 |1438.7333333333333333333333333 |1438.7333333333333333333333333|
|Two Bridges/Seward Park        |Bushwick South            |a2e0104dcf05c5e2dc114ebb96713c45 |1             |1438.2333333333333333333333333 |1438.2333333333333333333333333 |1438.2333333333333333333333333|
|Clinton East                   |Prospect-Lefferts Gardens |dc090740ef1915d3f9d2114e8d57fb21 |1             |1433.9333333333333333333333333 |1433.9333333333333333333333333 |1433.9333333333333333333333333|
|Lower East Side                |Sunset Park West          |6c7a35d40c37a2abdfe912b673fa1588 |1             |1250.5666666666666666666666667 |1250.5666666666666666666666667 |1250.56666666666              |
```

### Question 3

```sql
WITH LatestPickup AS (
    SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time FROM trip_data
    ),
    FilteredTrips AS (
        SELECT
            td.PULocationID,
            COUNT(*) AS pickup_count
        FROM
            trip_data td, LatestPickup lp
        WHERE
            td.tpep_pickup_datetime BETWEEN (lp.latest_pickup_time - INTERVAL '17 HOURS') AND lp.latest_pickup_time
        GROUP BY
            td.PULocationID
    )
    SELECT
        tz.zone,
        ft.pickup_count
    FROM
        FilteredTrips ft
    JOIN
        taxi_zone tz ON ft.PULocationID = tz.location_id
    ORDER BY
        ft.pickup_count DESC
```

```
|zone                          |count                    |
|------------------------------|-------------------------|
|LaGuardia Airport             |19                       |
|Lincoln Square East           |17                       |
|JFK Airport                   |17                       |

```



\
\

tried with streaming as well
```sql
CREATE OR REPLACE MATERIALIZED VIEW busiest_pickup_zones_last_17_hours AS
SELECT
    zone,
    COUNT(*) AS count
FROM
    trip_data td
JOIN
    taxi_zone tz
ON
    td.PULocationID = tz.location_id
GROUP BY
    zone
HAVING
    td.tpep_pickup_datetime >= ( select max(tpep_pickup_datetime) - interval '17 hours' from trip_data )
    -- td.tpep_pickup_datetime > max(td.tpep_pickup_datetime) - interval '17 hours'
ORDER BY
    count DESC;
```
got
```
[2024-03-18 13:59:10] [XX000] ERROR: Failed to execute the statement
[2024-03-18 13:59:10] Caused by:
[2024-03-18 13:59:10] Invalid input syntax: column must appear in the GROUP BY clause or be used in an aggregate function
```
wtf? what column? where? why? adapted dynamic filter from docs, ended up with unreasonable errors, gave up.