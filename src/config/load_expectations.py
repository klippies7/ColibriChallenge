# Databricks notebook source
query = f"""
    CREATE TABLE IF NOT EXISTS dbx_training_uc.config.pipeline_expectations (
    SourceName          string,
    TableName           string,
    Tier                string,
    ConstraintName      string,
    ConstraintCondition string
    )
 """
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dbx_training_uc.config.pipeline_expectations (SourceName, TableName, Tier, ConstraintName, ConstraintCondition) VALUES
# MAGIC   ("energysource", "turbine", "bronze", "valid_timestamp",  "timestamp IS NOT NULL"),
# MAGIC   ("energysource", "turbine", "bronze", "valid_turbine_id", "turbine_id IS NOT NULL"),
# MAGIC   ("energysource", "turbine", "bronze", "valid_wind_speed", "wind_speed IS NOT NULL"),
# MAGIC   ("energysource", "turbine", "bronze", "valid_wind_direction", "wind_direction IS NOT NULL"),
# MAGIC   ("energysource", "turbine", "bronze", "max_wind_direction", "wind_direction <= 360"),
# MAGIC   ("energysource", "turbine", "bronze", "valid_power_output", "power_output IS NOT NULL");