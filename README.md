# BDA Lab

## Spark run

### Matrix multiplication

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
