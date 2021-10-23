import sys

from pyspark import SparkContext, SparkConf 
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext, SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, DecimalType
import datetime;

if __name__ == "__main__":

    # Get Timestamp
    # ts = str(datetime.datetime.now().timestamp())

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Ratio Appartment - Formation07") 
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)
    spark = SparkSession(sc)

    # Vector assembler initialize
    vectorAssembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")

    # Collect Airbnb data
    airbnbDf = hc.sql("select * from formation07.airbnb_clean")

    # CSV Airbnb export
    airbnbDf.coalesce(1).write.mode('overwrite').option('header', 'true').csv("/user/formation07/airbnb_export.csv")

    # Create dataframe for clustering
    localisationDf = airbnbDf.select('latitude', 'longitude')
    localisationDfTransform = vectorAssembler.transform(localisationDf)

    # Create model to clustering by localisation
    kMeansNumber = 50
    clusteringKMeans = KMeans(k=kMeansNumber, seed=1)
    clusteringModel = clusteringKMeans.fit(localisationDfTransform)

    # Filter on specific data
    airbnbDf = airbnbDf.filter("room_type = 'Entire home/apt' and bedrooms = 1")

    # Airbnb link to predicted localisation data
    airbnbDfTransform = vectorAssembler.transform(airbnbDf)
    finalAirbnbDf = clusteringModel.transform(airbnbDfTransform)

    # Collect Cadastre data
    cadastreDf = hc.sql("select * from formation07.cadastre_clean")

    # CSV Cadastre export
    cadastreDf.coalesce(1).write.mode('overwrite').option('header', 'true').csv("/user/formation07/cadastre_export.csv")

    # Filter on specific data 
    filterCadastreDf = cadastreDf.filter("type_local = 'Appartement' and nombre_pieces_principales < 4")

    filterCadastreDfTransform = vectorAssembler.transform(filterCadastreDf)
    finalCadastreDf = clusteringModel.transform(filterCadastreDfTransform)
    
    # Grouping data
    groupByAirbnbDf = finalAirbnbDf.groupBy("prediction").mean("price")
    groupByCadastreDf = finalCadastreDf.groupBy("prediction").mean("price")

    groupByAirbnbDfUpdated = groupByAirbnbDf.withColumn("avg(price)", groupByAirbnbDf["avg(price)"].cast(IntegerType()))
    groupByAirbnbDfUpdated = groupByCadastreDf.withColumn("avg(price)", groupByCadastreDf["avg(price)"].cast(IntegerType()))

    # Merging datas
    centers = clusteringModel.clusterCenters()

    data = []

    schema = StructType([ \
    StructField("latitude",IntegerType(),True), \
    StructField("longitude",IntegerType(),True), \
    StructField("airbnbValue",IntegerType(),True), \
    StructField("cadastreValue", IntegerType(), True), \
    StructField("ratio", FloatType(), True), \
    ])

    i = 0

    for latitude, longitude in centers:

        airbnbFilteredValue = groupByAirbnbDfUpdated.filter("prediction == " + str(i))
        cadastreFilteredValue = groupByAirbnbDfUpdated.filter("prediction == " + str(i))

        if (len(airbnbFilteredValue.head(1)) > 0 and len(cadastreFilteredValue.head(1)) > 0):

            airbnbValue = airbnbFilteredValue.head(1)[0].__getitem__("avg(price)")
            cadastreValue = cadastreFilteredValue.head(1)[0].__getitem__("avg(price)")
            
            if (airbnbValue is not None and cadastreValue is not None):
                airbnbIntValue = int(airbnbValue)
                cadastreIntValue = int(cadastreValue)
                data.append((int(latitude), int(longitude), airbnbIntValue, cadastreIntValue, float(airbnbIntValue) / float(cadastreIntValue)))
            

        i = i + 1

    finalDf = spark.createDataFrame(data=data,schema=schema)
    finalDf.coalesce(1).write.mode('overwrite').option('header', 'true').csv("/user/formation07/data.csv")
