import pandas as pd
from sklearn.tree import DecisionTreeRegressor

# For visualization
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import seaborn as sns

airbnb_df = pd.read_csv('listings.csv') # here specify the path of the csv file

# input : print(airbnb_df.shape) / output : (10420, 74)

# input : print(airbnb_df.columns) / output : Index(['id', 'listing_url', 'scrape_id', 'last_scraped', 'name', 'description',
#        'neighborhood_overview', 'picture_url', 'host_id', 'host_url',
#        'host_name', 'host_since', 'host_location', 'host_about',
#        'host_response_time', 'host_response_rate', 'host_acceptance_rate',
#        'host_is_superhost', 'host_thumbnail_url', 'host_picture_url',
#        'host_neighbourhood', 'host_listings_count',
#        'host_total_listings_count', 'host_verifications',
#        'host_has_profile_pic', 'host_identity_verified', 'neighbourhood',
#        'neighbourhood_cleansed', 'neighbourhood_group_cleansed', 'latitude',
#        'longitude', 'property_type', 'room_type', 'accommodates', 'bathrooms',
#        'bathrooms_text', 'bedrooms', 'beds', 'amenities', 'price',
#        'minimum_nights', 'maximum_nights', 'minimum_minimum_nights',
#        'maximum_minimum_nights', 'minimum_maximum_nights',
#        'maximum_maximum_nights', 'minimum_nights_avg_ntm',
#        'maximum_nights_avg_ntm', 'calendar_updated', 'has_availability',
#        'availability_30', 'availability_60', 'availability_90',
#        'availability_365', 'calendar_last_scraped', 'number_of_reviews',
#        'number_of_reviews_ltm', 'number_of_reviews_l30d', 'first_review',
#        'last_review', 'review_scores_rating', 'review_scores_accuracy',
#        'review_scores_cleanliness', 'review_scores_checkin',
#        'review_scores_communication', 'review_scores_location',
#        'review_scores_value', 'license', 'instant_bookable',
#        'calculated_host_listings_count',
#        'calculated_host_listings_count_entire_homes',
#        'calculated_host_listings_count_private_rooms',
#        'calculated_host_listings_count_shared_rooms', 'reviews_per_month'],
#       dtype='object')

features = ['latitude', 'longitude', 'room_type', 'bedrooms', 'beds', 'price'] # list of features on which we will 

# Cleaning data on smaller dataframe

small_airbnb_df = airbnb_df[features].copy()

small_airbnb_df.loc[:, "price"] = small_airbnb_df["price"].apply(lambda x: float(x[1:].replace(',', ''))) # replacing comma for float with space because point already here

small_airbnb_df = small_airbnb_df.dropna(how="any", axis=0) # replace any field value by 0 if there is value "na"

small_airbnb_df = small_airbnb_df[small_airbnb_df.price < 500] # filtering where price of location for one night under 500 because extreme values

# Spliting room_type column into two columns to get int instead of string for entire home / private room 
small_airbnb_df["Entire_place"] = (small_airbnb_df["room_type"] == "Entire home/apt").astype(int)
small_airbnb_df["Private_room"] = (small_airbnb_df["room_type"] == "Private room").astype(int)
# improvable in only one column

new_features = ['latitude', 'longitude', 'Entire_place', 'Private_room', 'bedrooms', 'beds', 'price'] # adding new ccreated columns to features list

small_airbnb_df = small_airbnb_df[new_features].copy() # updating small_airbnb_df

# Creating decision tree regressor
dtr = DecisionTreeRegressor(min_samples_leaf=50) # creating decision tree with 50 leaves
dtr.fit(small_airbnb_df[new_features[:-1]], small_airbnb_df[new_features[-1]]) # training the tree with small_airbnb_df (without price) to determinate the price

# here read the cadastre csv file instead (just for test here we use airbnb dataframe)
cadastre_df = small_airbnb_df[new_features[:-1]].copy() # should be defined at the beginning of the script

# Clean also cadastre_df to have exact same columns name for features

# Adding a new column to cadastre df to predict the price from the previously trained dtr with airbnb data
# We now have the estimated price for which we can rent a property per year (per nights * 365)
cadastre_df["estimated_price"] = dtr.predict(cadastre_df[new_features[:-1]]) * 365

# Now in our dataframe we have the ratio of the price of the property bought by the estimated price per year that it can be rent on airbnb 
cadastre_df["ratio"] = dtr.predict()

# Missing part = Visualisation of the results (maybe creating clustering for better vision by neighbourhood)