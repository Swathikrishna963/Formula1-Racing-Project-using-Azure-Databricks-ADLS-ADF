-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Drop all the tables
-- MAGIC

-- COMMAND ----------

DROP DATABASE if EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1azure1dl/processed"

-- COMMAND ----------

DROP DATABASE if exists f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1azure1dl/presentation"