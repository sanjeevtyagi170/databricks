# Databricks notebook source
# df1 = spark.read.text("gs://rawdbrdata/datasets/autoloader/File 1.csv")
# df1.show()
# To check if we can access the files in this location

# COMMAND ----------

# .schema("name string, age int,department string,skills string") # If we provide schema then there will be no _rescued_data column
# .trigger(availableNow=True) # decides the batch or streaming
df=(
    spark.readStream
    .format("cloudFiles") # activate autoloader
    .option("cloudFiles.format","csv")
    .option("readerCaseSensitive","false") # it will read columns irrespective of the case
    .option("cloudFiles.schemaHints","Age int") # it is used to provide schema hints for columns
    .option("cloudFiles.schemaLocation","gs://rawdbrdata/checkpoint/") # checkpoint location
    .load("gs://rawdbrdata/datasets/autoloader") # input file
    .writeStream
    .option("checkpointLocation","gs://rawdbrdata/checkpoint/") # checkpoint location
    .option("mergeSchema","true")
    .trigger(processingTime='10 seconds')
    .toTable("test.testdb.autoloader_raw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.testdb.autoloader_raw
# MAGIC -- drop table  test.testdb.autoloader_raw;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


