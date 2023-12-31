# Databricks notebook source
class bronze():

    def __init__(self):
        self.base_data_dir="gs://rawdbrdata/raw/"
        self.bootstrap_server="pkc-n3603.us-central1.gcp.confluent.cloud:9092"
        self.jaas_module="org.apache.kafka.common.security.plain.PlainLoginModule"
        self.cluster_api_key='MWI4QIZ6IYPRG2IF'
        self.cluster_api_secret='C5REZQ79q4kjFPnhB4L5Yo/aK+xuhdMuM1TEGVyNFOLRpQgQrJ6lnCRYWejPPIKD'
    
    def ingestFromKafka(self,startingTime=1):
        return  (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", self.bootstrap_server)
                    .option("kafka.security.protocol","SASL_SSL")
                    # .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{self.cluster_api_key}' password='{self.cluster_api_secret}';")
                    .option("kafka.sasl.mechanism","PLAIN")
                    .option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + self.cluster_api_key + "\"password=\"" + self.cluster_api_secret + "\";")
                    .option("maxOffsetsPerTrigger",10) # number of records per batch
                    .option("subscribe", "invoices")
                    .option("startingTimestamp",startingTime)
                    .load()
                )
        
    def getInvoices(self,kafkaDf):
        from pyspark.sql.functions import cast, from_json
        return (
            kafkaDf.select(
                            kafkaDf.key.cast('string').alias("key"),
                            from_json(kafkaDf.value.cast("string"),self.getSchema()).alias("value"),
                            "topic",
                            "timestamp"
                        )
                )
            
    def getSchema(self):
        return """
                    InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                    CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                    PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                    DeliveryType string,
                    DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                    State string>,
                    InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
                """

    def process(self,startingTime=1):
        print(f"Starting Bronze Stream...", end='')
        rawDF = self.ingestFromKafka(startingTime)
        invoicesDF = self.getInvoices(rawDF)
        sQuery =  ( invoicesDF.writeStream
                        .queryName("bronze-ingestion")
                        .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/invoices_bz")
                        .outputMode("append")
                        .toTable("invoices_bz")           
                ) 
        print("Done")
        return sQuery  
obj=bronze()
obj.process()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from invoices_bz

# COMMAND ----------

invoicesDf

# COMMAND ----------


