# Databricks notebook source
bootstrap_server="pkc-n3603.us-central1.gcp.confluent.cloud:9092"
jaas_module="org.apache.kafka.common.security.plain.PlainLoginModule"
cluster_api_key='MWI4QIZ6IYPRG2IF'
cluster_api_secret='C5REZQ79q4kjFPnhB4L5Yo/aK+xuhdMuM1TEGVyNFOLRpQgQrJ6lnCRYWejPPIKD'


# COMMAND ----------

df = (spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrap_server)
  .option("kafka.security.protocol","SASL_SSL")
  # .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{cluster_api_key}' password='{cluster_api_secret}';")
  .option("kafka.ssl.endpoint.identification.algorithm","https")
  .option("kafka.sasl.mechanism","PLAIN")
  .option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + cluster_api_key + "\"password=\"" + cluster_api_secret + "\";")
  .option("subscribe", "invoices")
  .load()
)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + cluster_api_key + "\"password=\"" + cluster_api_secret + "\";"

# COMMAND ----------

f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{cluster_api_key}' password='{cluster_api_secret}';"

# COMMAND ----------

kafka_params = {
    "kafka.bootstrap.servers": "pkc-n3603.us-central1.gcp.confluent.cloud:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + cluster_api_key + "\"password=\"" + cluster_api_secret + "\";",
    "subscribe": "invoices"
}

df = spark \
    .read \
    .format("kafka") \
    .options(**kafka_params) \
    .load()
df.show()

# COMMAND ----------


