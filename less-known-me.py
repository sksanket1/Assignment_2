from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, IntegerType, StringType, StructField

spark=SparkSession.builder.appName("Less known").getOrCreate()

schema=StructType([\
                  StructField("id",IntegerType(),True),\
                  StructField("name",StringType(),True)])

names=spark.read.schema(schema).option("sep"," ").csv("file:///SparkCourse/Marvel+Names.txt")

lines=spark.read.text("file:///SparkCourse/Marvel+graph.txt")

connections=lines.withColumn("id",func.split(func.col("value")," ")[0])\
    .withColumn("connections",func.size(func.split(func.col("value")," "))-1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
mincount=connections.agg(func.min("connections")).first()[0]

minconnections=connections.filter(func.col("connections")==mincount)

minconnectionsWithNames=minconnections.join(names,"id")

print("The following characters have only"+str(mincount)+" connections:")

minconnectionsWithNames.select("name").show()