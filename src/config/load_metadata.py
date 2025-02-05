# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

query = f"""
    CREATE TABLE IF NOT EXISTS dbx_training_uc.config.pipeline_metadata
    (
            SourceName string,
            TableName string,
            KeyName string,
            Value string
    )
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC --energysource
# MAGIC --turbine
# MAGIC --schema
# MAGIC INSERT INTO
# MAGIC   dbx_training_uc.config.pipeline_metadata
# MAGIC VALUES
# MAGIC   (
# MAGIC     'energysource',
# MAGIC     'turbine',
# MAGIC     'schema',
# MAGIC     '[
# MAGIC         StructField("timestamp", TimestampType(), nullable=False),
# MAGIC         StructField("turbine_id", IntegerType(), nullable=False),
# MAGIC         StructField("wind_speed", FloatType(), nullable=False),
# MAGIC         StructField("wind_direction", IntegerType(), nullable=False),
# MAGIC         StructField("power_output", FloatType(), nullable=False)
# MAGIC     ]'
# MAGIC   );
# MAGIC INSERT INTO
# MAGIC   dbx_training_uc.config.pipeline_metadata
# MAGIC VALUES
# MAGIC   (
# MAGIC     'energysource',
# MAGIC     'turbine',
# MAGIC     'columnList',
# MAGIC     'timestamp,turbine_id,wind_speed,wind_direction,power_output'
# MAGIC   );
# MAGIC --keys
# MAGIC INSERT INTO
# MAGIC   dbx_training_uc.config.pipeline_metadata
# MAGIC VALUES
# MAGIC   (
# MAGIC     'energysource',
# MAGIC     'turbine', 
# MAGIC     'keys', 
# MAGIC     'timestamp,turbine_id');
# MAGIC --sequence key
# MAGIC INSERT INTO
# MAGIC   dbx_training_uc.config.pipeline_metadata
# MAGIC VALUES
# MAGIC   (
# MAGIC     'energysource',
# MAGIC     'turbine',
# MAGIC     'sequence_by',
# MAGIC     'timestamp'
# MAGIC   );