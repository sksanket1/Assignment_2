from pyspark import SparkConf, SparkContext

conf=SparkConf().setMaster("local").setAppName("Mintemp")
sc=SparkContext(conf=conf)

def parseLine(line):
    fields= line.split(',')
    stationID=fields[0]
    entryType=fields[2]
    temprature=float(fields[3])
    return(stationID,entryType,temprature)
    
lines=sc.textFile("file:///SparkCourse/1800.csv")
parsedLines=lines.map(parseLine)
minTemps=parsedLines.filter(lambda x:"TMIN" in x[1])
stationTemps=minTemps.map(lambda x:(x(0),x[2]))
mintemps=stationTemps.reduceByKey(lambda x,y:min(x,y))
results=mintemps.collect();

for result in results:
    print(result[0]+ "\t{:.2f}C").format(result[1])
    