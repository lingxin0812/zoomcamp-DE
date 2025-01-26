# zoomcamp-DE
Code/Query for HW

-- question 1<br />
$ docker run -it --entrypoint=bash python:3.12.8<br />
/# pip --version<br />
>pip 24.3.1<br />

-- question 3<br />
select count(trip_distance)<br />
from green_trip<br />
where trip_distance<=1;<br />

select count(trip_distance)<br />
from green_trip<br />
where trip_distance>1 and trip_distance<=3;<br />

select count(trip_distance)<br />
from green_trip<br />
where trip_distance>3 and trip_distance<=7;<br />

select count(trip_distance)<br />
from green_trip<br />
where trip_distance>7 and trip_distance<=10;<br />

select count(trip_distance)<br />
from green_trip<br />
where trip_distance>10;<br />

-- question 4<br />
select lpep_pickup_datetime::DATE, trip_distance<br />
from green_trip<br />
where trip_distance = (select max(trip_distance) from green_trip);<br />

-- question 5<br />
select b."Zone", sum(a.total_amount) as Total_amount<br />
from green_trip a<br />
left join taxi_zones b<br />
on a."PULocationID" = b."LocationID"<br />
where a.lpep_pickup_datetime::DATE = '2019-10-18'<br />
group by a."PULocationID" ,b."Zone"<br />
having sum(Total_amount) >13000<br />
order by Total_amount desc;<br />

-- question 6<br />
select c."Zone"<br />
from green_trip a<br />
left join taxi_zones b<br />
on a."PULocationID" = b."LocationID"<br />
left join taxi_zones c<br />
on a."DOLocationID" = c."LocationID"<br />
where b."Zone" = 'East Harlem North'<br />
and a.tip_amount = (<br />
    select max(a2.tip_amount)<br />
    from green_trip a2<br />
    left join taxi_zones b2<br />
    on a2."PULocationID" = b2."LocationID"<br />
    where b2."Zone" = 'East Harlem North');<br />
