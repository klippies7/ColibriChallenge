# Databricks notebook source
from pyspark.sql.functions import col, first, avg, when, lit, min, max, stddev, mean

# COMMAND ----------

def anomolychecks(df):
    # Calculate min, max, and average power output over the *entire* dataset for each turbine
    df_turbine_stats = df.groupBy("turbine_id").agg(
        min("power_output").alias("min_power"),
        max("power_output").alias("max_power"),
        avg("power_output").alias("avg_power")
    )

    # Calculate mean and standard deviation (over the entire data) for anomaly detection.
    df_global_stats = df.groupBy("timestamp", "turbine_id").agg(
        mean("power_output").alias("mean"),
        stddev("power_output").alias("std")
    )

    # Handle cases where std is 0
    df_global_stats = df_global_stats.withColumn("std", when(col("std") == 0, lit(1e-9)).otherwise(col("std")))

    # Calculate bounds
    df_global_stats = df_global_stats.withColumn("lower_bound", col("mean") - 2 * col("std"))
    df_global_stats = df_global_stats.withColumn("upper_bound", col("mean") + 2 * col("std"))

    # Join with original data (and min/max/avg)
    df_merged = df.join(df_turbine_stats, "turbine_id", "left")\
        .join(df_global_stats, ["timestamp", "turbine_id"], "left")

    # Identify anomalies and add the 'anomaly_detected' column
    anomalous_turbines = df_merged.withColumn(
        "anomaly_detected",
        (col("power_output") < col("lower_bound")) | (col("power_output") > col("upper_bound"))
    )

    return anomalous_turbines.select("timestamp", "turbine_id", "power_output", "anomaly_detected", "min_power", "max_power", "avg_power")

# COMMAND ----------

# DBTITLE 1,Get Pipeline MetaData
# Load in the entire source. ideally, this would be a streaming from the same source in Bronze and 
# Silver but DLT is yet to support writing to multiple target schemas.(it is on the way)

silver_source_df = spark.read.table("dbx_training_uc.silver.silver_energysource_turbine")

# Execute the anomoly detection
anomoly_df = anomolychecks(silver_source_df)

# perform full update to target
anomoly_df.write.mode("overwrite").saveAsTable("dbx_training_uc.gold.gold_anomoly_turbine")