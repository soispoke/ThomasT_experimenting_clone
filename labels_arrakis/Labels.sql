-- Databricks notebook source
-- MAGIC %md #Labels
-- MAGIC 
-- MAGIC Scripts generating labels populating the current labels table can be found in this github repo: https://github.com/duneanalytics/ThomasT_experimenting/tree/main/labels_arrakis

-- COMMAND ----------

-- MAGIC %md Create Labels table 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE FUNCTION get_labels(_address STRING)
-- MAGIC 
-- MAGIC RETURNS STRING
-- MAGIC 
-- MAGIC RETURN
-- MAGIC     SELECT
-- MAGIC     concat_ws(', ',array_agg(distinct blockchain),array_agg(DISTINCT name)) as label_name
-- MAGIC     FROM default.all_labels_table labels
-- MAGIC     WHERE (labels.address = get_labels._address);

-- COMMAND ----------

SELECT 
from as address,
get_labels(from) as labels
FROM jonas10_ethereum.transactions
LIMIT 10

-- COMMAND ----------

SELECT 
DISTINCT
from as address,
get_labels(from)
FROM jonas10_ethereum.transactions
WHERE (get_labels(from)) ilike '%Top 5%'
LIMIT 10

-- COMMAND ----------

SELECT 
DISTINCT
from as address,
get_labels(from)
FROM jonas10_ethereum.transactions
WHERE (get_labels(from)) ilike '%Exchange%'
LIMIT 10

-- COMMAND ----------

SELECT 
DISTINCT
from as address,
get_labels(from)
FROM jonas10_ethereum.transactions
WHERE (get_labels(from)) ilike '%Vitalik Buterin%'

-- COMMAND ----------

-- MAGIC %md Create get_labels function

-- COMMAND ----------

-- MAGIC %python
-- MAGIC all_labels = spark.sql('''SELECT * FROM default.all_labels''')
-- MAGIC 
-- MAGIC def fill_na_labels(all_labels):
-- MAGIC   return all_labels.fillna((''),("metric")).fillna((0),("quantity")).fillna((100),("percentile")).fillna((10e31),("rank"))
-- MAGIC 
-- MAGIC spark.udf.register("fill_na_labels", fill_na_labels,StringType())

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC 
-- MAGIC def get_labels(address, blockchain = None, category = None, name = None, metric = None, details = False, quantity = 0, percentile = 5, rank = 10e31):
-- MAGIC   # Fill null values for categorical labels (Identity, Address Type, etc...)  
-- MAGIC   # Filter based on arguments passed in the function
-- MAGIC   labels = spark.sql('''SELECT * FROM default.all_labels''').fillna((''),("metric")).fillna((0),("quantity")).fillna((0),("percentile")).fillna((10e31),("rank"))
-- MAGIC   # Here, for example, none of the arguments are passed except for the address
-- MAGIC   if blockchain == None and category == None and name == None and metric == None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & (col("quantity") >= quantity) & (col("percentile") <= percentile)\
-- MAGIC                                                        & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC   elif blockchain != None and category != None and name != None and metric != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & (col("blockchain") == blockchain)\
-- MAGIC                                                                                           & (col("category") == category)\
-- MAGIC                                                                                           & (col("name") == name) & (col("metric") == metric)\
-- MAGIC                                                                                           & (col("quantity") >= quantity) & (col("percentile") <= percentile)\
-- MAGIC                                                                                           & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC   elif blockchain != None and category != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & (col("blockchain") == blockchain) & (col("category") == category)\
-- MAGIC                                                                       & (col("quantity") >= quantity) & (col("percentile") <= percentile)\
-- MAGIC                                                                       & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC   elif blockchain != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & (col("blockchain") == blockchain) & (col("quantity") >= quantity)\
-- MAGIC                                                                      & (col("percentile") <= percentile)\
-- MAGIC                                                                      & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC   elif category != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & ((col("address") == address) & col("category") == category)
-- MAGIC                                                                       & (col("quantity") >= quantity) & (col("percentile") <= percentile)\
-- MAGIC                                                                       & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC   elif name != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & (col("name") == name) & (col("quantity") >= quantity)\
-- MAGIC                                                                         & (col("percentile") <= percentile)\
-- MAGIC                                                                         & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC 
-- MAGIC   elif blockchain != None and name != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & (col("blockchain") == blockchain) & (col("name") == name)\
-- MAGIC                                                                       & (col("quantity") >= quantity) & (col("percentile") <= percentile)\
-- MAGIC                                                                       & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC     return ', '.join(x) 
-- MAGIC   elif category != None and name != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) & (col("category") == category) & (col("name") == name)\
-- MAGIC                                                                                    & (col("quantity") >= quantity) & (col("percentile") <= percentile)\
-- MAGIC                                                                                    & (col("rank") <= rank)).withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect() 
-- MAGIC   elif blockchain != None and category != None and name != None:
-- MAGIC     x = labels.select("name").where((col("address") == address) &  (col("blockchain") == blockchain) & (col("category") == category)\
-- MAGIC                                                                      & (col("name") == name) & (col("quantity") >= quantity) & (col("percentile") <= percentile)\
-- MAGIC                                                                      & (col("rank") <= rank))\
-- MAGIC                                                             .withColumnRenamed("concat(blockchain,  , name)","name")\
-- MAGIC                                                             .select("name").rdd.flatMap(lambda x: x).collect()
-- MAGIC   return ', '.join(x) 
-- MAGIC 
-- MAGIC # Example addresses w/ multiple labels: 0x2910543af39aba0cd09dbb2d50200b3e800a63d2,0x1151314c646ce4e0efd76d1af4760ae66a9fe30f,0xab5801a7d398351b8be11c439e05c5b3259aec9b
-- MAGIC label = get_labels('0xab5801a7d398351b8be11c439e05c5b3259aec9b')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.udf.register("GetLabelsPython", get_labels,StringType())

-- COMMAND ----------

CREATE OR REPLACE VIEW identity_labels AS

SELECT * FROM identity_funds
UNION ALL
SELECT * FROM identity_exchanges
UNION ALL
SELECT * FROM identity_exchanges


-- COMMAND ----------

-- MAGIC %md All Labels Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TABLE all_labels_table AS
-- MAGIC 
-- MAGIC SELECT * FROM address_type_labels
-- MAGIC UNION ALL
-- MAGIC SELECT * FROM gas_labels
-- MAGIC UNION ALL
-- MAGIC SELECT * FROM identity_labels
