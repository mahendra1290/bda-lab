from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("exp").getOrCreate()

sc = spark.sparkContext

mat_a = sc.textFile("mat_a.txt")
mat_b = sc.textFile("mat_b.txt")

def transform(val):
  row = sc.parallelize(val.split(',')).map(int)
  return row

mat_a_data = mat_a.map(transform).collect()

mat_b_data = mat_b.map(val).collect()

print(mat_a_data, mat_b_data)

mat_b_cols = mat_b.map(lambda line: line.split(" "))

# mat_a_rows.saveAsTextFile("output.txt")
