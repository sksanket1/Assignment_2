from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,LongType

spark=SparkSession.builder.appName("PopularMovies").getOrCreate()

schema=StructType([\
                   StructField("userid", IntegerType(),True),\
                   StructField("movieid", IntegerType(), True),\
                   StructField("rating", IntegerType(), True),\
                   StructField("timestamp", IntegerType(),True)
                   ])

moviesDF=spark.read.option("sep","\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

topmoviesid=moviesDF.groupBy("movieid").count().orderBy(func.desc("count"))

topmoviesid.show(10)

spark.stop()