from pyspark.sql import SparkSession
import os
import spark


def getCreateTableScriptCSV(databaseName, tableName, path, df):
    cols = df.dtypes
    createScript = "CREATE EXTERNAL TABLE IF NOT EXISTS " + databaseName + "." + tableName + "("
    colArray = []
    for colName, colType in cols:
        colArray.append(colName.replace(" ", "_") + " " + colType)
    createColsScript =   ", ".join(colArray )
    
    script = createScript + createColsScript + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '" + path + "' TBLPROPERTIES('skip.header.line.count'='1') "
    print (script)
    return script

def createTableCSV(databaseName, tableName, path): 
    df = spark.read.format("csv").option("header", "true").option("inferschema","true").load(path)
    sqlScript = getCreateTableScriptCSV(databaseName, tableName, path, df)
    spark.sql(sqlScript)



# Set path where the csv file located.
my_csv_file_path = os.path('urlset.csv')
createTableCSV("default","csv_table",my_csv_file_path)




# Set path where parquet folder with csv files inside located.
folder_path = os.path('outputs')
createTableCSV("default","table_from_dir2",folder_path)


databaseName = "default"
filepath = os.path('outputs')

for fileOrDir in os.listdir(filepath):
    if fileOrDir.endswith(".csv") :
        createTableCSV(databaseName, fileOrDir.split(".csv")[0], filepath.replace("/v3io/", "v3io://", 1) + "/" + fileOrDir)
    elif not fileOrDir.startswith(".") :
        createTableCSV(databaseName, fileOrDir, filepath.replace("/v3io/", "v3io://", 1) + "/" + fileOrDir + "/*")






# test how the tables were saved
#spark.sql("drop database test CASCADE")
spark.sql("show databases").show()
spark.sql("show tables in " + databaseName).show()




# test how saving to table works
tableName = "example1"
spark.sql("select * from " + databaseName + "." + tableName)


spark.sql("drop table " + databaseName + ".example1")
