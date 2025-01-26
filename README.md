# zoomcamp-DE
Query for HW

--question3
select count(trip_distance)
from green_trip
where trip_distance<=1;

select count(trip_distance)
from green_trip
where trip_distance>1 and trip_distance<=3;

select count(trip_distance)
from green_trip
where trip_distance>3 and trip_distance<=7;

select count(trip_distance)
from green_trip
where trip_distance>7 and trip_distance<=10;

select count(trip_distance)
from green_trip
where trip_distance>10;

--question4
select lpep_pickup_datetime::DATE, trip_distance
from green_trip
where trip_distance = (select max(trip_distance) from green_trip);

--question5
select b."Zone", sum(a.total_amount) as Total_amount
from green_trip a
left join taxi_zones b
on a."PULocationID" = b."LocationID"
where a.lpep_pickup_datetime::DATE = '2019-10-18'
group by a."PULocationID" ,b."Zone"
having sum(Total_amount) >13000
order by Total_amount desc;

--question6
select c."Zone"
from green_trip a
left join taxi_zones b
on a."PULocationID" = b."LocationID"
left join taxi_zones c
on a."DOLocationID" = c."LocationID"
where b."Zone" = 'East Harlem North'
and a.tip_amount = (
    select max(a2.tip_amount)
    from green_trip a2
    left join taxi_zones b2
    on a2."PULocationID" = b2."LocationID"
    where b2."Zone" = 'East Harlem North');
