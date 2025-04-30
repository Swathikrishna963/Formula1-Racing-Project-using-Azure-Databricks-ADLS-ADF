# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 : Read Results.json file using the spark dataframe reader 

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType([StructField("resultId", IntegerType(), False),
                             StructField("raceId", IntegerType(), True),
                             StructField("driverId", IntegerType(), True),
                             StructField("constructorId", IntegerType(), True),
                             StructField("number", IntegerType(), True),
                             StructField("grid", IntegerType(), True),
                             StructField("position", IntegerType(), True),
                             StructField("positionText", StringType(), True),
                             StructField("positionOrder", IntegerType(), True),
                             StructField("points", FloatType(), True),
                             StructField("laps", IntegerType(), True),
                             StructField("time", StringType(), True),
                             StructField("milliseconds", IntegerType(), True),
                             StructField("fastestLap", IntegerType(), True),
                             StructField("rank", IntegerType(), True),
                             StructField("fastestLapTime", StringType(), True),
                             StructField("fastestLapSpeed", StringType(), True),
                             StructField("statusId", IntegerType(), True)])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 : Rename and add new columns to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_df)

# COMMAND ----------

results_with_column_df = results_with_ingestion_date_df.withColumnRenamed("resultId", "result_id") \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("constructorId", "constructor_id") \
                                   .withColumnRenamed("positionText", "position_text") \
                                   .withColumnRenamed("positionOrder", "position_order") \
                                   .withColumnRenamed("fastestLap", "fastest_lap") \
                                   .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                   .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                   .withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 : Drop unwanted columns to the dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

results_final_df = results_with_column_df.drop(col("statusId"))

# COMMAND ----------

result_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 : Write the dataframe in to the processed container

# COMMAND ----------

# MAGIC %md
# MAGIC ####method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")


# COMMAND ----------

# MAGIC %md
# MAGIC ####method 2

# COMMAND ----------

#overwrite_partition(results_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(result_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.results group by race_id order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id,count(1) from f1_processed.results group by race_id,driver_id 
# MAGIC having count(1)>1
# MAGIC order by race_id desc,driver_id desc;
# MAGIC     
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")