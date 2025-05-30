# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team") \
    .withColumnRenamed("nationality", "constructor_nationality")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("name", "result_name") \
    .withColumnRenamed("number", "race_number") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

# MAGIC %md 
# MAGIC ####join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ####join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id, "inner") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select(race_results_df.race_id,race_results_df.race_year, race_results_df.race_name, race_results_df.race_date, race_results_df.circuit_location, race_results_df.driver_name, race_results_df.driver_number, race_results_df.driver_nationality, race_results_df.team, race_results_df.grid,race_results_df.race_time,race_results_df.fastest_lap, race_results_df.points, race_results_df.position,race_results_df.result_file_date ) \
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date") 

# COMMAND ----------



#overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_presentation.race_results group by race_id order by race_id desc;

# COMMAND ----------

