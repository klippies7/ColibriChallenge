# Databricks notebook source
import dlt
from pyspark.sql.functions import col, first
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,DLT Raw To Clean
def load_turbine_bronze_to_silver(sourceName, tableName, keys, sequence_by):

  # -----------------------------------------------------
  # Select data not marked for quarantine from Raw task
  # -----------------------------------------------------
  @dlt.view(
      name = f"valid_{sourceName}_{tableName}"
  )
  def get_valid_rows():
    return (
      dlt.read_stream(f"bronze_{sourceName}_{tableName}")
        .filter("is_quarantined = false")
    )

  # -----------------------------------------------------
  # Write data marked for quarantine from Bronze task
  # -----------------------------------------------------
  @dlt.table(
      name = f"dbx_training_uc.bronze.quarantine_{sourceName}_{tableName}",
      spark_conf = {"spark.databricks.delta.schema.autoMerge.enabled": "true"}
  )
  def get_quarantine_rows():
    return (
      dlt.read_stream(f"bronze_{sourceName}_{tableName}")
        .filter("is_quarantined = true")
    )

  # -----------------------------------------------------
  # Write data marked for quarantine from Raw task
  # -----------------------------------------------------
  dlt.create_streaming_table(
      name = f"dbx_training_uc.silver.silver_{sourceName}_{tableName}",
      spark_conf = {"spark.databricks.delta.schema.autoMerge.enabled": "true"}
  )

  # -----------------------------------------------------
  # Load data using CDC
  # -----------------------------------------------------
  dlt.apply_changes(
      source = f"valid_{sourceName}_{tableName}",
      target = f"dbx_training_uc.silver.silver_{sourceName}_{tableName}", 
      keys = keys.split(","),
      sequence_by = col(f"{sequence_by}"),
      except_column_list = ["is_quarantined"]
  )

# COMMAND ----------

# DBTITLE 1,Get Pipeline MetaData
#---------------------------------------------------------------------
# Read the metadata to support multiple tables per source if needs be
#---------------------------------------------------------------------
sourceName = 'energysource'
dfPipelineMetaData = (spark.read
                           .format('delta')
                           .table("dbx_training_uc.config.pipeline_metadata")
                           .where(f"SourceName == '{sourceName}'")
                           .select('SourceName','TableName','KeyName','Value'))

dfPivot = dfPipelineMetaData.groupBy('SourceName','TableName').pivot('KeyName').agg(first("Value"))

pandas_df = dfPivot.toPandas()

# COMMAND ----------

# DBTITLE 1,Process each Table
#------------------------------------
# Compile the pipeline activities per table
#------------------------------------
for index, row in pandas_df.iterrows():
    sourceName  = row['SourceName'].lower()
    tableName   = row['TableName'].lower()
    keys        = row['keys']
    sequenceBy  = row['sequence_by']

    load_turbine_bronze_to_silver(sourceName, tableName, keys, sequenceBy)