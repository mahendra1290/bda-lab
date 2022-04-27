import pyspark
from pyspark.sql import SparkSession
import operator

import os

sc = pyspark.SparkContext(appName="Matrix Multiplication")

matrix_a_raw = sc.textFile("matrixA.txt")

matrix_b_raw = sc.textFile("matrixB.txt")

spark = SparkSession(sc)

def to_matrix_a(x):
    i, j, v = x.split()
    return (j, (i, v))

def to_matrix_b(x):
    j, k, v = x.split()
    return (j, (k, v))

def to_matrix_entry(x):
    i, j, v = x.split()
    return MatrixEntry(i, j, v)

entries_a = matrix_a_raw.map(to_matrix_a)

entries_b = matrix_b_raw.map(to_matrix_b)


print(entries_a.collect())

print(entries_b.collect())


def multiply_mat(x):
    left = x[1][0]
    right = x[1][1]
    i, v = left
    k, w = right
    return ((i, k), (int(v) * int(w)))

product_entries = entries_a \
    .join(entries_b) \
    .map(multiply_mat) \
    .reduceByKey(operator.add) \
    .map(lambda x:  (x[0][0], x[0][1], x[1]))

print(product_entries.collect())
