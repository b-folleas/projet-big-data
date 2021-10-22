```sh
hdfs dfs -mkdir -p hive/airbnb

hive-beeline

create external table airbnb (
  id int,
  neighbourhood_cleansed string,
  latitude double,
  longitude double,
  room_type string,
  bedrooms int,
  beds int,
  price string,
  has_availability string,
  availability_30 int,
  availability_60 int,
  availability_90 int,
  availability_365 int,
  calendar_last_scraped string,
  calculated_host_listings_count int,
  calculated_host_listings_count_entire_homes int,
  calculated_host_listings_count_private_rooms int,
  calculated_host_listings_count_shared_rooms int
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/user/formation07/hive/airbnb/'
tblproperties ("skip.header.line.count"="1");

show tables;
ctrl c;

hdfs dfs –copyFromLocal airbnb_filtered.csv hive/airbnb

hive-beeline

select * from airbnb;

create table airbnb_clean (
  id int,
  neighbourhood_cleansed string,
  latitude double,
  longitude double,
  room_type string,
  bedrooms int,
  beds int,
  price float,
  has_availability string,
  availability_30 int,
  availability_60 int,
  availability_90 int,
  availability_365 int,
  calendar_last_scraped string,
  calculated_host_listings_count int,
  calculated_host_listings_count_entire_homes int,
  calculated_host_listings_count_private_rooms int,
  calculated_host_listings_count_shared_rooms int
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile
tblproperties ("skip.header.line.count"="1");

insert into airbnb_clean (id,neighbourhood_cleansed,latitude,longitude,room_type,bedrooms,beds,price,has_availability,availability_30,availability_60,availability_90,availability_365,calendar_last_scraped,calculated_host_listings_count,calculated_host_listings_count_entire_homes,calculated_host_listings_count_private_rooms,calculated_host_listings_count_shared_rooms)
select id,neighbourhood_cleansed,latitude,longitude,room_type,bedrooms,beds,cast(cast(replace(price, '$', '') as float) * 0.859187 * availability_365 as float) as price,
case has_availability when 't' then 1 else 0 end,
availability_30,availability_60,availability_90,availability_365,calendar_last_scraped,calculated_host_listings_count,calculated_host_listings_count_entire_homes,calculated_host_listings_count_private_rooms,calculated_host_listings_count_shared_rooms
from airbnb;

```

