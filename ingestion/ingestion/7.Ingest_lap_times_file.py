# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest lap times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 1 : read the folder containing multiple csv files using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType



# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
                                       ])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 2 : rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                     .withColumnRenamed("raceId", "race_id") \
                                     .withColumn("data_source", lit(v_data_source)) \
                                     .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3 : Write the output to the processed container in parquet format

# COMMAND ----------

#overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id and tgt.race_id = src.race_id and tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.lap_times group by race_id order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")