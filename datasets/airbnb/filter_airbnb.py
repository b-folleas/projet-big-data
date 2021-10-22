import pandas as pd

airbnb_df = pd.read_csv('airbnb.csv')

columns_to_keep = [ 'id','neighbourhood_cleansed','latitude','longitude','room_type','bedrooms','beds','price','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms']

airbnb_df = airbnb_df[columns_to_keep].copy()

print(airbnb_df.to_string())

airbnb_df.to_csv('airbnb_filtered.csv', index=False)