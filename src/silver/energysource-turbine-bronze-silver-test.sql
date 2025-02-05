-- Databricks notebook source
-- Databricks notebook source
CREATE TEMPORARY LIVE TABLE test_energysource_turbine (
  CONSTRAINT keep_all_rows EXPECT (num_rows = 14) ON VIOLATION FAIL UPDATE
)
AS
  WITH
    rows_test AS (SELECT count(*) AS num_rows FROM dbx_training_uc.silver.silver_energysource_turbine)
  SELECT * FROM rows_test

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE test_quarantine_energysource_turbine (
  CONSTRAINT Row_Count           EXPECT (Row_Count = 6)            ON VIOLATION FAIL UPDATE,
  CONSTRAINT timestamp1          EXPECT (timestamp1 = 5)           ON VIOLATION FAIL UPDATE,
  CONSTRAINT wind_speed          EXPECT (wind_speed = 5)           ON VIOLATION FAIL UPDATE,
  CONSTRAINT wind_direction      EXPECT (wind_direction = 5)       ON VIOLATION FAIL UPDATE,
  CONSTRAINT wind_direction_max  EXPECT (wind_direction_max = 1)   ON VIOLATION FAIL UPDATE,
  CONSTRAINT power_output        EXPECT (power_output = 5)         ON VIOLATION FAIL UPDATE
)
AS
  WITH
    Row_Count_test AS (SELECT count(*) AS Row_Count                   FROM dbx_training_uc.bronze.quarantine_energysource_turbine),
    timestamp_test AS (SELECT count(*) AS timestamp1                  FROM dbx_training_uc.bronze.quarantine_energysource_turbine WHERE timestamp IS NOT NULL),
    wind_speed_test AS (SELECT count(*) AS wind_speed                 FROM dbx_training_uc.bronze.quarantine_energysource_turbine WHERE wind_speed IS NOT NULL),
    wind_direction_test AS (SELECT count(*) AS wind_direction         FROM dbx_training_uc.bronze.quarantine_energysource_turbine WHERE wind_direction IS NOT NULL),
    wind_direction_max_test AS (SELECT count(*) AS wind_direction_max FROM dbx_training_uc.bronze.quarantine_energysource_turbine WHERE wind_direction > 360),
    power_output_test AS (SELECT count(*) AS power_output             FROM dbx_training_uc.bronze.quarantine_energysource_turbine WHERE power_output IS NOT NULL)
  SELECT * FROM Row_Count_test, timestamp_test, wind_speed_test, wind_direction_test, wind_direction_max_test, power_output_test