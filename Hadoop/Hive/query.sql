create database taxi_db;
show databases;
use taxi_db;
set hive.execution.engine=tez;

create table if not exists taxi_data
(
	vendor_id string,
	tpep_pickup_datetime timestamp,
	tpep_dropoff_datetime timestamp,
	passenger_count int,
	trip_distance double,
	ratecode_id int,
	store_and_fwd_flag string,
	pulocation_id int,
	dolocation_id int,
	payment_type int,
	fare_amount double,
	extra double,
	mta_tax double,
	tip_amount double,
	tolls_amount double,
	improvement_surcharge double,
	total_amount double,
	congestion_surcharge double
)
row format delimited
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");

load data local inpath '/home/ubuntu/taxi_data/' into table taxi_data;

select * from taxi_data limit 10;

select
	vendor_id,
	avg(passenger_count) as avg_passenger_count,
	min(trip_distance) as min_trip_distance,
	max(trip_distance) as max_trip_distance,
	avg(total_amount) as avg_total_amount
from taxi_data
where vendor_id is not null and vendor_id <> ''
group by vendor_id;
