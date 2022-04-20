from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("bda_lab_exp5").getOrCreate()

sc = spark.sparkContext

filename = "data.csv"

df = spark.read.option("header",True).csv(filename)


marksColumns = [col('marks1'), col('marks2')]

averageFunc = sum(x for x in marksColumns)/len(marksColumns)
# rdd2=df.rdd.map(lambda x: (f'{x[0]},{x[1]},{x[2]},{x[3]},{x[1]+x[2]+x[3]}'))
rdd2=df.rdd.map(lambda x: (x[0],x[1],x[2],x[3],str(int(x[1])+int(x[2])+int(x[3]))))
df2=rdd2.toDF(["name","marks1", "marks2", "marks3", "total"]   )
# df2=rdd2.toDF(["name","marks1", "marks2"])
df2.show()

df.withColumn('Result(Avg)', averageFunc).show(truncate=False)
df.show()
