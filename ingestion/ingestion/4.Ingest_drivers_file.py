# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file 

# COMMAND ----------

# MAGIC %md 
# MAGIC ####step 1 : Read drivers.json file using the spark dataframe reader 
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(), True), 
                          StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId", IntegerType(), False), 
                             StructField("driverRef", StringType(), True),
                             StructField("number", IntegerType(), True),
                              StructField("code", IntegerType(), True),
                             StructField("name", name_schema, True),
                             StructField("dob", DateType(), True),
                             StructField("nationality", StringType(),True),
                             StructField("url", StringType(),True)])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 2 : Rename the columns,add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. Add ingestion_date as new column
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 : drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 4 : Write the output to the processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")