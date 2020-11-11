from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField, IntegerType, LongType
import codecs

def loadMoviesNames():
    movieNames={}
    with codecs.open("C:/SparkCourse/ml-100k/u.data","r",encoding='ISO-8859-1',errors='ignore') as f:
        for line in f:
            fields=line.split('|')
            movieNames[int(fields[0])]=fields[1]
    return movieNames

spark=SparkSession.builder.appName("Famous Movies").getOrCreate()

namedict=spark.sparkContext.broadcast(loadMoviesNames())

schema=StructType([\
                   StructField("userid",IntegerType(),True),\
                   StructField("movieid",IntegerType(),True),\
                   StructField("rating",IntegerType(),True),\
                   StructField("timestamp",LongType(),True)])

movieDF=spark.read.option("sep","\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

moviecounts=movieDF.groupBy("movieid").count()

def lookupName(movieid):
    return nameDict.value[movieid]

lookupnameUDF=func.udf(lookupname)

movieswithnames=moviecounts.withColumn("movietitle",lookupnameUDF(func.col("movieid")))

sortedmovieswithnames=movieswithnames.orderBy(func.desc("count"))

sortedmovieswithnames.show(10,False)

spark.stop()    