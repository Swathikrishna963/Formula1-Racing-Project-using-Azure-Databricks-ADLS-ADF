# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest multiple multi line JSON file 

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 1 : read the JSON folder using the spark dataframe reader

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

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("member", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)
                                       ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 2 : rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

qualifying_final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
                                     .withColumnRenamed("raceId", "race_id") \
                                     .withColumnRenamed("driverId", "driver_id") \
                                     .withColumnRenamed("constructorId", "constructor_id") \
                                     .withColumn("data_source", lit(v_data_source)) \
                                     .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3 : Write the output to the processed container in parquet format

# COMMAND ----------

#overwrite_partition(qualifying_final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.qualifying group by race_id order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

