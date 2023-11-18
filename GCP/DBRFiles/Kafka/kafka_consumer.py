# Databricks notebook source
bootstrap_server="pkc-n3603.us-central1.gcp.confluent.cloud:9092"
# jaas_module="org.apache.kafka.common.security.plain.PlainLoginModule"
jaas_module="kafkashaded.org.apache.common.security.plain.PlainLoginModule"
cluster_api_key="ZKP5URHC7O4FOIQK"
cluster_api_secret="yMjcbjJvepEfTHVouAUlwFcjapvPtbjQxY+5fXto9hNpLwLOhvJTUjokNwROV5gN"
df = (spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrap_server)
  # .option("kafka.security.protocol","SASL_SSL")
  # .option("kafka.ssl.mechanism","PLAIN")
  # # .option("kafka.sasl.jaas.config",f"{jaas_module} required username='{cluster_api_key}' password='{cluster_api_secret}';")
  # .option("kafka.sasl.jaas.config","{} required username='{}' password='{}';".format(jaas_module,cluster_api_key,cluster_api_secret))
  .option("subscribe", "invoices")
  .load()
)


# COMMAND ----------

display(df)

# COMMAND ----------


