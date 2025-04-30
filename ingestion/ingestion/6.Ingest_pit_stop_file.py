# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Multi line JSON file

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 1 : read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType



# COMMAND ----------

pit_stop_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("stop", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("duration", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
                                       ])

# COMMAND ----------

pit_stop_df = spark.read.schema(pit_stop_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 2 : rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stop_df)

# COMMAND ----------

pit_stop_with_column_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                     .withColumnRenamed("raceId", "race_id") \
                                     .withColumn("data_source", lit(v_data_source)) \
                                     .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3 : Write the output to the processed container in parquet format

# COMMAND ----------

#overwrite_partition(pit_stop_with_column_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id and tgt.race_id = src.race_id and tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stop_with_column_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.pit_stops group by race_id order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")