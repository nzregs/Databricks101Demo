// Databricks notebook source
// MAGIC %md
// MAGIC #### List directory in Azure Blob Storage (publically accessible)

// COMMAND ----------

// MAGIC %fs ls "wasbs://towerdata@difinitytel.blob.core.windows.net/"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Show the head of a file, to see the structure

// COMMAND ----------

// MAGIC %fs head "wasbs://towerdata@difinitytel.blob.core.windows.net/2018/08/01/00/0_0abcdcb8ab73475a82d2e82784ffeaf3_1.json"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Mount Azure Blob Storage into Databricks
// MAGIC This will create a local /mnt/mountname which will be accessible to all clusters and notebooks

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://towerdata@difinitytel.blob.core.windows.net/",
  mountPoint = "/mnt/towerdata",
  extraConfigs = Map("fs.azure.account.key.difinitytel.blob.core.windows.net" -> "OJxjnbtxSN/m6IHhXkV3/rGa6kfT86iv**S3CR3TK3YH1DD3N**m9boX6XeG0HfmaGcvewPTrQQB71KHXom8qA=="))

// COMMAND ----------

// MAGIC %fs ls "/mnt/towerdata"

// COMMAND ----------

// MAGIC %fs head "/mnt/towerdata/2018/08/12/23/0_b7bc7ed1b47b4302bdc8ec77515ab7a4_1.json"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Define a schema to use when loading data
// MAGIC You can omit the schema and Databricks will "infer schema" but if you explicitly specify schema then dataframe are faster to create and you will likely have less data type casting errors if the inferred schema didn't use enough sample rows when it was generated

// COMMAND ----------

import org.apache.spark.sql.types._

//{"eventdate":"2018-08-01T00:00:29.0000000Z","fromnumber":"215556413","tonumber":null,"billingtype":"data","subscriberid":null,"firstname":"Monique ","lastname":"Baldwin","uri":"twitter.com","towerid":"100003","towername":"BLACKPOOL","toweraddress":"10 Moa Ave, Oneroa, Auckland 1081, New Zealand","towercity":"Auckland","towerlongitude":"175.0132661","towerlatitude":"-36.7861592","bytes":965.0,"dataplan":"Twitter","offer":"free social networking","costpermb":0.0,"duration":0}

val cdrSchema = StructType(Array(
        StructField("eventdate", StringType, true),
        StructField("fromnumber", StringType, true),
        StructField("tonumber", StringType, true),
        StructField("billingtype", StringType, true),
        StructField("subscriberid", StringType, true),
        StructField("firstname", StringType, true),
        StructField("lastname", StringType, true),
        StructField("uri", StringType, true),
        StructField("towerid", StringType, true),
        StructField("towername", StringType, true),
        StructField("toweraddress", StringType, true),
        StructField("towercity", StringType, true),
        StructField("towerlongitude", StringType, true),
        StructField("towerlatitude", StringType, true),
        StructField("bytes", DoubleType, true),
        StructField("dataplan", StringType, true),
        StructField("offer", StringType, true),
        StructField("costpermb", DoubleType, true),
        StructField("duration", IntegerType, true)))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read Json file into a Dataframe

// COMMAND ----------

val cdrDF = sqlContext.read.schema(cdrSchema).json("/mnt/towerdata/2018/08/12/23/0_b7bc7ed1b47b4302bdc8ec77515ab7a4_1.json")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Display the data in the Dataframe
// MAGIC Note - the dataframe load didn't happen until now... spark is 'lazy' - one of the things that makes it fast :-)

// COMMAND ----------

display(cdrDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read multiple files into Dataframe

// COMMAND ----------

val biggerDF = sqlContext.read.schema(cdrSchema).json("/mnt/towerdata/2018/08/20/*/*.json")

// COMMAND ----------

display(biggerDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Count the rows in the Dataframe

// COMMAND ----------

biggerDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Filter and aggregate the data in the Dataframe 

// COMMAND ----------

import org.apache.spark.sql.functions._

val newDF = biggerDF
  .select("towername", "towercity","bytes", "uri", "eventdate")
  .filter("billingType = 'data'")
  .filter("eventdate between '2018-08-20' and '2018-08-21'")
  .groupBy("uri", "towername", "towercity")
  .agg(sum("bytes"))
  

// COMMAND ----------

display(newDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Switch to using SQL
// MAGIC 
// MAGIC Either __present existing dataframe read in via scala as a SQL table__ 

// COMMAND ----------

// MAGIC %scala cdrDF.createOrReplaceTempView("smallcdrdata")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   towername, towercity, sum(bytes)
// MAGIC from
// MAGIC   smallcdrdata
// MAGIC group by
// MAGIC   towername, towercity

// COMMAND ----------

// MAGIC %md
// MAGIC ####Switch to SQL
// MAGIC or __load json data directly into SQL table__

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS sqlcdrdata; 
// MAGIC CREATE TEMPORARY TABLE sqlcdrdata
// MAGIC   USING json
// MAGIC   OPTIONS (path "/mnt/towerdata/2018/08/12/23/0_b7bc7ed1b47b4302bdc8ec77515ab7a4_1.json", mode "FAILFAST");
// MAGIC SELECT * FROM sqlcdrdata;

// COMMAND ----------

// MAGIC %md
// MAGIC ####Or use python

// COMMAND ----------

// MAGIC %python
// MAGIC pythonDF = spark.read.json("/mnt/towerdata/2018/08/12/23/0_b7bc7ed1b47b4302bdc8ec77515ab7a4_1.json")
// MAGIC display(pythonDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ####R is supported too

// COMMAND ----------

// MAGIC %r
// MAGIC 
// MAGIC require(SparkR)
// MAGIC 
// MAGIC rDF <- sql("SELECT * FROM smallcdrdata")
// MAGIC print(count(rDF))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Later on....
// MAGIC 
// MAGIC #####Structured Streaming with Regan
// MAGIC #####Machine Learning with Chimene