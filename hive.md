```sh
hdfs dfs -mkdir -p hive/cadastre
hdfs dfs -mkdir -p hive/airbnb

hive-beeline

create external table cadastre (
  id_mutation string,
  date_mutation string,
  nature_mutation string,
  valeur_fonciere float,
  adresse_numero int,
  adresse_suffixe string,
  adresse_nom_voie string,
  adresse_code_voie string,
  code_postal int,
  code_commune string,
  nom_commune string,
  code_departemen string,
  lot1_surface_carrez float,
  surface_reelle_bati float,
  nombre_pieces_principales int,
  surface_terrain float,
  longitude float,
  latitude float
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/user/formation07/hive/cadastre/'
tblproperties ("skip.header.line.count"="1");

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

hdfs dfs –copyFromLocal cadastre_filtered.csv hive/cadastre
hdfs dfs –copyFromLocal airbnb_filtered.csv hive/airbnb

hive-beeline

select * from cadastre;
select * from airbnb;

# filter Vente
create table cadastre_wip1 as select * from cadastre where nature_mutation='Vente';

create table cadastre_wip2 (
  id_mutation string,
  date_mutation string,
  nature_mutation string,
  valeur_fonciere float,
  adresse_numero int,
  adresse_suffixe string,
  adresse_nom_voie string,
  adresse_code_voie string,
  code_postal int,
  code_commune string,
  nom_commune string,
  code_departemen string,
  lot1_surface_carrez float,
  surface_reelle_bati float,
  nombre_pieces_principales int,
  surface_terrain float,
  longitude float,
  latitude float
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile
tblproperties ("skip.header.line.count"="1");

insert into cadastre_wip2 (id_mutation,date_mutation,nature_mutation,valeur_fonciere,adresse_numero,adresse_suffixe,adresse_nom_voie,adresse_code_voie,code_postal,code_commune,nom_commune,code_departemen,lot1_surface_carrez,surface_reelle_bati,nombre_pieces_principales,surface_terrain,longitude,latitude)
select id_mutation,date_mutation,nature_mutation,valeur_fonciere,
case adresse_numero when 'NA' then cast('0' as int) else cast(adresse_numero as int) end,
adresse_suffixe,adresse_nom_voie,adresse_code_voie,
case code_postal when 'NA' then cast('0' as int) else cast(code_postal as int) end,
code_commune,nom_commune,
case nombre_pieces_principales when 'NA' then cast('0' as int) else cast(nombre_pieces_principales as int) end,
from flights_wip1;

```

