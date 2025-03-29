from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# --- Configuration Variables ---
# Replace with your actual values
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_CONN_ID = "google_cloud_default"  # Your Airflow Connection ID for GCP
BQ_LOCATION = "US" # Or your specific BigQuery region (e.g., 'us-central1', 'EU')

# Stored Procedure Details
SP_DATASET = "your_dataset"
SP_NAME = "usp_generic_merge" # The name of your generic merge procedure

# Table Details for the SP call
TARGET_TABLE = f"{GCP_PROJECT_ID}.{SP_DATASET}.target_customers"
SOURCE_TABLE = f"{GCP_PROJECT_ID}.{SP_DATASET}.staging_customers"
KEY_COLUMNS = ['customer_id'] # Must be a list for the ARRAY<STRING> type in BQ SP
# --- End Configuration ---

# Format the key columns list into a BQ ARRAY<STRING> literal
# Note: Each string within the array needs single quotes
key_columns_sql_array = "[" + ", ".join([f"'{col}'" for col in KEY_COLUMNS]) + "]"

# Construct the CALL statement
# Note the backticks around the procedure name
# Arguments are passed positionally
# JSON argument is passed as a string literal 'null' or '{}'
sql_call_statement = f"""
CALL `{GCP_PROJECT_ID}.{SP_DATASET}.{SP_NAME}`(
    '{TARGET_TABLE}',          -- target_table_name STRING
    '{SOURCE_TABLE}',          -- source_table_name STRING
    {key_columns_sql_array},   -- key_columns ARRAY<STRING>
    NULL                       -- options JSON (using NULL here)
    -- Or use an empty JSON object: PARSE_JSON('{{}}')
);
"""

# Alternative using PARSE_JSON for the options argument if you need an empty JSON object
# sql_call_statement = f"""
# CALL `{GCP_PROJECT_ID}.{SP_DATASET}.{SP_NAME}`(
#     '{TARGET_TABLE}',
#     '{SOURCE_TABLE}',
#     {key_columns_sql_array},
#     PARSE_JSON('{{}}') -- options JSON (using empty JSON)
# );
# """


with DAG(
    dag_id="bq_call_generic_merge_sp",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    schedule=None, # Or set your desired schedule
    tags=["bigquery", "stored-procedure", "example"],
) as dag:
    call_bigquery_stored_procedure = BigQueryExecuteQueryOperator(
        task_id="call_usp_generic_merge",
        sql=sql_call_statement,
        use_legacy_sql=False,
        location=BQ_LOCATION,       # Specify the location of your dataset/procedure
        gcp_conn_id=GCP_CONN_ID,    # Your configured Google Cloud connection ID
        # You can add other parameters like:
        # priority='INTERACTIVE',
        # labels={"airflow-task": "call_usp_generic_merge"},
    )
