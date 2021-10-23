import pandas as pd

filepath = '../../datasets/cadastre/cadastre.csv'
columns = ['type_local', 'nombre_pieces_principales']

df = pd.read_csv(filepath)

gkk = df.groupby(columns).size().reset_index(name='counts')

gkk = gkk.sort_values(by=['counts'], ascending=False)

df_final = gkk.head(10)

###
# gkk = df.groupby(columns)['valeur_fonciere'].mean().reset_index(name='mean')
# gkk = gkk.sort_values(by=['valeur_fonciere'], ascending=False)
# df_final = gkk.head(20)
###

print(df_final)