-- Databricks notebook source
CREATE DATABASE if not exists f1_raw;

-- COMMAND ----------

drop table if exists f1_raw.circuits;
CREATE TABLE if not exists f1_raw.circuits
(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string
)
USING csv
OPTIONS(path "/mnt/formula1azure1dl/raw/circuits.csv", header true);


-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(raceId int,
 year int,
 round int,
circuitId int,
name string,
date date,
time string,
url string)
USING CSV
options(path "/mnt/formula1azure1dl/raw/races.csv", header true) 

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###create tables for json file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####create constructors table 
-- MAGIC - single json file
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(constructorId int,
 constructorRef string,
 name string,
 nationality string,
 url string)
 using JSON
 options(path "/mnt/formula1azure1dl/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### create drivers table
-- MAGIC - single json file
-- MAGIC - complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers
(driverId int,
 driverRef string,
 number int,
 code string,
 name struct<forename:string, surname:string>,
 dob date,
 nationality string,
 url string)
 using JSON
 options(path "/mnt/formula1azure1dl/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####create results table
-- MAGIC - single json
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(resultId int,
 raceId int,
 driverId int,
 constructorId int,
 number int,
 grid int,
 position int,
 positionText string,
 positionOrder int,
 points double,
 laps int,
 time string,
 milliseconds int,
 fastestLap int,
 rank int,
 fastestLapTime string,
 fastestLapSpeed string,
 statusId int)
 using JSON
 options(path "/mnt/formula1azure1dl/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####create pits stop table
-- MAGIC - Multi line structure
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(driverId int,
duration string,
 lap int,
 milliseconds int,
 raceId int,
 stop int,
  time string)
  USING JSON
  options(path "/mnt/formula1azure1dl/raw/pit_stops.json", multiLine true)


-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###create tables for the list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####create lap times table
-- MAGIC - Multiple files
-- MAGIC - CSV Format

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(driverId int,
raceId int,
lap int,
position int,
time string,
milliseconds int)
 using CSV
 options(path "/mnt/formula1azure1dl/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####create qualifying table
-- MAGIC - multiple files
-- MAGIC - Multiline json
-- MAGIC - JSON file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(qualifyId int,
raceId int,
driverId int,
constructorId int,
number int,
position int,
q1 string,
q2 string,
q3 string)
 using JSON
 options(path "/mnt/formula1azure1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

describe extended f1_raw.qualifying;

-- COMMAND ----------

