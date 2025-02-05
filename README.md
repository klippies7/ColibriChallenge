# ColibriChallenge

## Intro

This challenge has been submitted by Sean Niemeyer

## Solution

A Databricks workspace with Unity Catalog is being populated by means of Delta Live Tables. The Delta Live Tables solution is configured by means of metadata for both the DLT tables and Expectations per table. This allows for a repeated approach to be taken should there be multiple tables per source. The data not meeting Expectations will be delivered to a quarantine table. This is a pattern suggested by Databricks. But now there is a lab call DQX which can expand on this pattern

## Unity Catalog

The Unity Catalog is setup as follows:

- Bronze
  - Volume (test) - Stores the test file used to test Exceptions in DLT.
  - Volume (turbine) - Stores the 3 files provided for ingestion.
  - Table (Quarantine) - Stores data that has failed the Expectations in DLT

- Silver
  - Table (Turbine) - Stores data that has made it past the Expections in DLT

- Gold
  - Table (Turbine) - Stored Turbine data marked with anomalies

## Execution

There are Jobs to perform the Production workloads as well as for the Test workload

- Production
  - The metadata for DLT tables and DLT Expectations are loaded. Normally they would be files delivered from the repo.
- Testing
  - The Job executes just the following:
    - Load bespoke test file
    - Execute the Production DLT process
    - Test the outcomes for Quarantine and Silver table
