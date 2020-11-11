from pyspark import SparkConf, SparkContext

conf=SparkConf().setMaster("local").setAppName("Total_spent_by_Customers")
sc=SparkContext(conf=conf)

def parseLine(line):
    fields=line.split(',')
    customerid=int(fields[0])
    spent=float(fields[2])
    return(customerid,spent)
    
lines=sc.textFile("file:///SparkCourse/customer-orders.csv")
mappedinput=lines.map(parseLine)
totalbycustomer=mappedinput.reduceByKey(lambda x,y:x+y)
results=totalbycustomer.collect();
for result in results:
    print(result)   
    
    
