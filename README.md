# BDA Lab

## Spark run

### Matrix multiplication

Matrix files are matrixA.txt and matrixB.txt

```
$ cd spark
$ spark-submit matrix.py
```

### Run hive

launch hive first start hadoop using

```
$ start-dfs.sh
$ start-yarn.sh
```

```
$ hive
```

run file

```
$ hive -f [filename]
$ hive -f sample.sql
```

### Run pig

```
$ pig -x local [scriptname]
$ pig -x exp-10/script.pig
```
