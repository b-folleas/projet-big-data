```sh
hdfs dfs -mkdir -p hive/cadastre
hdfs dfs -rm hive/cadastre/cadastre_filtered.csv
hdfs dfs -copyFromLocal cadastre_filtered.csv hive/cadastre

hive-beeline

drop table cadastre;
drop table cadastre_wip1;
drop table cadastre_clean;

create external table cadastre (
  id int,
  id_mutation string,
  date_mutation string,
  nature_mutation string,
  valeur_fonciere float,
  adresse_numero int,
  adresse_suffixe string,
  adresse_nom_voie string,
  adresse_code_voie string,
  code_postal int,
  nom_commune string,
  code_departement string,
  lot1_surface_carrez float,
  surface_reelle_bati float,
  type_local string,
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

# remove duplicate id_mutation
select id_mutation, count(*) from cadastre group by id_mutation; # multiple
create table cadastre_wip1 as select * from cadastre where id not in (select distinct t1.id from cadastre t1 JOIN cadastre t2 ON t2.id_mutation = t1.id_mutation AND t2.id < t1.id);
select id_mutation, count(*) from cadastre_wip1 group by id_mutation; # only one


# filter Vente
create table cadastre_wip2 as select * from cadastre_wip1 where nature_mutation='Vente' and code_postal like '6900_' and valeur_fonciere not in (0,1) and longitude is not null and latitude is not null;

create table cadastre_clean (
  id_mutation string,
  date_mutation string,
  nature_mutation string,
  price float,
  adresse_numero int,
  adresse_suffixe string,
  adresse_nom_voie string,
  adresse_code_voie string,
  code_postal int,
  nom_commune string,
  code_departement string,
  lot1_surface_carrez float,
  surface_reelle_bati float,
  type_local string,
  nombre_pieces_principales int,
  surface_terrain float,
  longitude float,
  latitude float
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile
tblproperties ("skip.header.line.count"="1");

insert into cadastre_clean (id_mutation,date_mutation,nature_mutation,price,adresse_numero,adresse_suffixe,adresse_nom_voie,adresse_code_voie,code_postal,nom_commune,code_departement,lot1_surface_carrez,surface_reelle_bati,type_local,nombre_pieces_principales,surface_terrain,longitude,latitude)
select id_mutation,date_mutation,nature_mutation,valeur_fonciere, coalesce(adresse_numero, 0), adresse_suffixe,adresse_nom_voie,adresse_code_voie, coalesce(code_postal, 0),nom_commune,code_departement, lot1_surface_carrez,surface_reelle_bati,type_local,coalesce(nombre_pieces_principales, 0),coalesce(surface_terrain,0),round(longitude, 5),round(latitude, 5)
from cadastre_wip2;
```

