## Question 1
--> It applies a limit 100 only to our staging models.

 option 1 is also technically correct,\
 since is_test_run variable is true by default and applies a limit 100 only to our staging models anyway.\
 So both 1 and 3 are correct.
 
## Question 2
--> The code from a development branch requesting a merge to main

## Question 3
--> 22998722

in my case it was 23,014,060, that is the closest option.

## Question 4
Full amount of data:
* green
  * all - 8,035,161
  * filtered - 6,776,572
* yellow
  * all - 109,247,536
  * filtered - 56,525,825
* fhv
  * all - 43,261,276
  * filtered - 43,261,273
  
*filtered refers to the filers applied to staging models.

Rides Performed by each service on 2019.07:\
Yellow - 3,249,672\
Green - 415,281\
FHV - 290,682

--> Yellow

queries used: 
```sql
with data as (
  select *,
  timestamp_trunc(
        cast(pickup_datetime as timestamp),
        month
    ) as trip_month, 
  from `peerless-sensor-411315`.`dbt_dev_ny_taxi_rides_dimi`.`fact_fhv_trips`
)
select count(1),
from data
where trip_month = '2019-07-01 00:00:00 UTC'
```
```sql
with data as (
  select *,
  timestamp_trunc(
        cast(pickup_datetime as timestamp),
        month
    ) as revenue_month, 
  from `peerless-sensor-411315`.`dbt_dev_ny_taxi_rides_dimi`.`fact_trips`
)
select service_type,
count(1)
from data 
where revenue_month = '2019-07-01 00:00:00 UTC'
group by service_type
```

