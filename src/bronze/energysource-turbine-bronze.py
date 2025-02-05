# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, current_timestamp, substring_index, regexp_replace, input_file_name, first, col
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Function to Load Expectations
#------------------------------------
# Retrieve and compile the expectations
#------------------------------------
def get_rules(sourceName, tableName, tier):
  rules = {}
  df = (spark.read
             .table("dbx_training_uc.config.pipeline_expectations")
             .where(f"SourceName = '{sourceName}' AND TableName = '{tableName}' AND Tier = '{tier}'"))

  for row in df.collect():
    rules[row['ConstraintName']] = row['ConstraintCondition']
  return rules

# COMMAND ----------

# DBTITLE 1,Rules
#------------------------------------
# Compile the expectations
#------------------------------------
rules = get_rules("energysource","turbine","bronze")
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

# COMMAND ----------

# DBTITLE 1,DLT Ingest Raw
def load_turbine_bronze(sourceName, tableName, tableSchema, columnList, quarantine_rules):
    # Format the schema
    stringSchema = f'StructType({tableSchema})'
    customSchema = eval(stringSchema)
    bronzedataPath = f"/Volumes/dbx_training_uc/bronze/turbine/"

    @dlt.view(
        name = f"bronze_{sourceName}_{tableName}",
        spark_conf = {"spark.sql.files.ignoreMissingFiles": "true"}
    )
    # Get autoloader to read in new files
    def incremental_raw():
        df = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
                .options(header="true")
                .schema(customSchema)
                .load(bronzedataPath)
                .select(columnList)
        )

        #------------------------------------
        # Add derived columns
        #------------------------------------
        df = (df.withColumn("sys_source_file", col("_metadata.file_path"))
                .withColumn("sys_window_path", regexp_replace(substring_index(substring_index("sys_source_file", "/", -6), "/", 5), r'[A-z]*=',''))
                .withColumn("sys_insert_time_utc", current_timestamp())
                .withColumn("is_quarantined", expr(quarantine_rules))
        )
        
        return df

# COMMAND ----------

# DBTITLE 1,Get Metadata
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

# DBTITLE 1,Load all Tables
#------------------------------------
# Compile the pipeline activities per table
#------------------------------------
for index, row in pandas_df.iterrows():
    sourceName  = row['SourceName'].lower()
    tableName   = row['TableName']
    schema      = row['schema']
    columnList  = row['columnList'].split(",")

    load_turbine_bronze(sourceName, tableName, schema, columnList, quarantine_rules)