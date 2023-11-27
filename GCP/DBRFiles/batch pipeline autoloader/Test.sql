-- Databricks notebook source
Select * from songs

-- COMMAND ----------

Select count(*) from raw_songs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/databricks-datasets/songs/data-001/")
