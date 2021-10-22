```sh
hdfs dfs -mkdir -p hive/cadastre

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
  code_departement string,
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

show tables;
ctrl c;

hdfs dfs –copyFromLocal cadastre_filtered.csv hive/cadastre

hive-beeline

select * from cadastre;

# filter Vente
create table cadastre_wip1 as select * from cadastre where nature_mutation='Vente';

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
  code_commune string,
  nom_commune string,
  code_departement string,
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

insert into cadastre_clean (id_mutation,date_mutation,nature_mutation,price,adresse_numero,adresse_suffixe,adresse_nom_voie,adresse_code_voie,code_postal,code_commune,nom_commune,code_departement,lot1_surface_carrez,surface_reelle_bati,nombre_pieces_principales,surface_terrain,longitude,latitude)
select id_mutation,date_mutation,nature_mutation,valeur_fonciere, coalesce(adresse_numero, 0), adresse_suffixe,adresse_nom_voie,adresse_code_voie, coalesce(code_postal, 0), code_commune,nom_commune,code_departement, lot1_surface_carrez,surface_reelle_bati,coalesce(nombre_pieces_principales, 0),surface_terrain,round(longitude, 2),round(latitude, 5)
from cadastre_wip1;
```

