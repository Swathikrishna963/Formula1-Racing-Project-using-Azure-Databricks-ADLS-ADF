-- Databricks notebook source
select 
team_name,
count(1) as total_races,
sum(calculated_points) total_points,
avg(calculated_points) avg_points
from f1_presentation.calculated_race_results
group by team_name
having count(1) >= 100
order by avg_points desc

-- COMMAND ----------

