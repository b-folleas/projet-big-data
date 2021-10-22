import sys

from pyspark import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import when, lit
from pyspark.ml.regression import DecisionTreeRegressor

# Execute with : spark-submit --master yarn spark-job.py
# Test with : spark-shell --master yarn --conf spark.ui.port=40XX --executor-memory 512M --executor-cores 1 --num-executors 1

if __name__ == "__main__":

    # Create a SparkSession
    spark = (SparkSession
    .builder
    .appName("AppName")
    .getOrCreate())

    # url or path to data set
    airbnb_csv_file = "http://data.insideairbnb.com/france/auvergne-rhone-alpes/lyon/2021-09-14/visualisations/listings.csv"
    # !!! Lyon 1 only !!! # TODO: extend to other districts
    cadastre_csv_file = "https://files.data.gouv.fr/geo-dvf/latest/csv/2020/communes/69/69001.csv"

    # Read csv file to create temporary view
    airbnb_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss") 
    .load(airbnb_csv_file))
    
    cadastre_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss") 
    .load(cadastre_csv_file))

    features = ['latitude', 'longitude', 'room_type', 'bedrooms', 'beds', 'price'] # list of features on which we will apply decision tree regressor

    ### Cleaning data ###

    ## Cleaning small_airbnb_df ##

    # create sub dataframe from airbnb_df with only columns stored in features
    small_airbnb_df = airbnb_df.select(*features)

    # replace any field value by 0 if there is value "na"
    small_airbnb_df = small_airbnb_df.dropna(how="any", axis=0)

    # filtering where price of location for one night under 500 because extreme values
    small_airbnb_df = small_airbnb_df.filter(small_airbnb_df.price < 500)

    # Changing room_type column with 1 if entire home/apt and 0 if private room
    small_airbnb_df = small_airbnb_df.withColumn("room_type", when((small_airbnb_df["room_type"] == "Entire home/apt"), lit(1)).otherwise(lit(0)))

    # Removing duplicates in dataframe
    small_airbnb_df = small_airbnb_df.dropDuplicates()

    ## Cleaning cadastre_df ## TODO

    # ...

    ### Creating decision tree regressor ###

    dtr = DecisionTreeRegressor()

    # training the tree by the features (except last one : price) to determinate price
    model = dtr.fit(small_airbnb_df[features[:-1]], small_airbnb_df[features[-1]])

    # Adding a new column to cadastre_df to predict the price from the previously trained dtr with airbnb data
    # We now have the estimated price for which we can rent a property per year (per nights * 365)
    cadastre_df.withColumn("estimated_price", (model.predict(cadastre_df[features[:-1]]) * 365))

    # Adding a new column to cadastre_df to have the ratio from the estimated price by the price of property purchased
    cadastre_df.withColumn("estimated_price", cadastre_df.select(cadastre_df["estimated_price"]) / cadastre_df.select(cadastre_df["price"]).astype(int))
    
    # Write cleaned cadastre_df with estimated price and ratio as csv file on hairbnb_dfs
    # cadastre_df.write.csv("hdfs:///user/path_of_cleaned_file.csv")

    ### Then display data using one of the Hadoop visualization Tools ### 