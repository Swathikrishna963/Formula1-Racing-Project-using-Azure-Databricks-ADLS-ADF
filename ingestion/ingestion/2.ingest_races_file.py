# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 1 : Read csv file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

race_schema = StructType(fields =[StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(race_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column as required 

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Concatenate the date and time to a new column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, lit, concat, col

# COMMAND ----------

races_transformed_df = races_renamed_df.withColumn('race_timestamp',to_timestamp(concat(col("date"),lit(" "),col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Select only the columns required

# COMMAND ----------

races_selected_df = races_transformed_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), col("name"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_selected_df)

# COMMAND ----------

races_final_df = races_with_ingestion_date_df.withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")