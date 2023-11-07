import pandas as pd
import numpy as np
import dbldatagen as dg
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("syntheticdata") \
    .config("spark.executor.memory", "8g")\
    .getOrCreate()

from pyspark.sql.types import IntegerType, FloatType, StringType,LongType
from dbldatagen import DataGenerator, fakerText
column_count = 1
data_rows = 1000 * 1000
my_word_list = [
'danish','cheesecake','sugar',
'Lollipop','wafer','Gummies',
'sesame','Jelly' ] # Faker word list
df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,partitions=4)
           .withIdOutput()
           .withColumn("amount", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=column_count)
           .withColumn("department_code", IntegerType(), minValue=100, maxValue=200)
           .withColumn("address", text=fakerText("address" ))# Faker
           .withColumn("code2", LongType(), minValue=201141015171810, maxValue=2121345678971810)
           .withColumn("code3", StringType(), values=['aa', 'ba', 'ac','ba','ca','cc'])
           .withColumn("email", text=fakerText("ascii_company_email") )
           .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
           .withColumn("faker_text", text=fakerText("sentence", ext_word_list=my_word_list,)) # Faker
           )
df = df_spec.build()
num_rows=df.count()
print(num_rows)
df.show()                     