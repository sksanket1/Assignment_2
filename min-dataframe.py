from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])
df= spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
df.printSchema()

minTemps=df.filter(df.measure_type=="TMIN")

stationTemps=minTemps.select("stationID","temperature")

minTempsByStation=stationTemps.groupBy("stationID").min("temperature")

minTempsByStation.show()

minTempsByStationC=minTempsByStation.withColumn("temperature"
    ,func.round(func.col("min(temperature)")*0.1)).select("stationID","temperature").sort("temperature")

results=minTempsByStationC.collect()

for result in results:
    print(result[0]+"\t{:.2f}C".format(result[1]))

spark.stop()