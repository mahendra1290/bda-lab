from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("bda_lab_exp5").getOrCreate()

sc = spark.sparkContext

filename = "/home/maahi/BDA_LAB/exp-6/README.md"
text_file = sc.textFile(filename)

counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x+y)

output = counts.collect()

print("Word counts: ")
for (word, count) in output:
    print("%s: %i"%(word, count))
