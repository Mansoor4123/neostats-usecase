# ASG Airlines - End-to-End Data Engineering Case Study

Submission for the NeoStats Data Engineering Internship use-case round.

## What this is

An end-to-end pipeline that ingests ASG Airlines' operational data (flights, bookings, payments,
and passengers), cleans and standardizes it, protects sensitive passenger information, and
produces an analytics-ready dataset visualized in a 4-page Power BI dashboard.

The written problem statement describes only the flights table's columns, but the actual data
provided contains four related sheets (flights, bookings, payments, passengers), connected through
flight_id, booking_id, and passenger_id. All four were used throughout this pipeline, since all
four were part of the data actually given.

## Folder structure

```
notebook/           - the full Databricks/PySpark pipeline (ingestion -> cleaning -> PII masking ->
                       join logic -> star schema -> export), with markdown explaining every step
cleaned_data/        - the exported Gold-layer tables (star schema + one flat table for BI)
powerbi/             - the .pbix dashboard file and a screenshot of each of its 4 pages
documentation/       - full write-up: architecture, data flow, data model, cleaning logic and
                       assumptions, PII/access control, and dashboard walkthrough (.docx)
```

## How to run the pipeline

1. Import `notebook/asg_airlines_pipeline.py` into a Databricks workspace (Workspace -> Import)
2. Upload the 4 source CSVs into a Unity Catalog Volume (path referenced at the top of the notebook)
3. Attach to a compute resource and Run All
4. The notebook exports the 4 cleaned tables found in `cleaned_data/`

## Key findings during data cleaning

The dataset contained several deliberately-realistic data quality issues, each diagnosed and fixed
with reasoning documented in both the notebook and the full documentation:

- 15 exact duplicate flight records
- A flight identifier (`6F250`) reused across two genuinely different flights
- One flight with an impossible negative duration (arrival before departure)
- Two inconsistent representations of "unknown airline" (blank vs the literal text "UNKNOWN")
- Bookings with a null status and a separate "INVALID" status category
- Payment amounts recorded as the literal text "INVALID", and bookings with up to 6 separate
  payment records each (no single-payment-per-booking assumption held)
- A passenger identifier that was not actually unique - 36 passenger_ids had 2-3 conflicting rows
- Two referential-integrity issues that only appeared after joining tables together: an ambiguous
  flight reference (from the `6F250` collision) and a dangling reference to a flight that had
  already been removed as invalid

Full reasoning for every decision above is in `documentation/ASG_Airlines_Documentation.docx`.

## Tools used

Azure Databricks (PySpark, Serverless Compute) for ingestion, cleaning, transformation, and PII
masking; Azure Data Lake Storage / Unity Catalog Volumes for storage; Power BI Desktop for the
dashboard. See the documentation for why Databricks alone was chosen over ADF/Synapse for this
use case.
