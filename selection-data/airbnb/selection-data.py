import pandas as pd

filepath = '../../datasets/airbnb/airbnb.csv'
columns = ['property_type', 'room_type','accommodates','bedrooms','beds']

df = pd.read_csv(filepath)

gkk = df.groupby(columns).size().reset_index(name='counts')

gkk = gkk.sort_values(by=['counts'], ascending=False)

df_final = gkk.head(10)

print(df_final)