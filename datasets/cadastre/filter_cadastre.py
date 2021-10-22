import pandas as pd

cadastre_df = pd.read_csv('cadastre.csv')

columns_to_keep = ['id_mutation','date_mutation','nature_mutation','valeur_fonciere','adresse_numero','adresse_suffixe','adresse_nom_voie','adresse_code_voie','code_postal','nom_commune','code_departement','lot1_surface_carrez','surface_reelle_bati','type_local', 'nombre_pieces_principales','surface_terrain','longitude','latitude']

cadastre_df = cadastre_df[columns_to_keep].copy()

print(cadastre_df.to_string())

cadastre_df.to_csv('cadastre_filtered.csv', index=False)