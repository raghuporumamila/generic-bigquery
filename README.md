# generic-bigquery-merge

## merge.sql
creating a truly generic MERGE statement in pure static SQL isn't possible because SQL requires explicit table and column names. However, you can achieve reusability in BigQuery using Dynamic SQL within Scripting or Stored Procedures.

### This approach involves:

Querying Metadata: Using INFORMATION_SCHEMA.COLUMNS to get the column names for your source and target tables.

Constructing the SQL String: Building the MERGE statement dynamically as a string, incorporating the retrieved column names.

Executing the Dynamic SQL: Using EXECUTE IMMEDIATE to run the constructed statement.

Here's how you can implement this using a BigQuery Stored Procedure, which is the most common way to encapsulate reusable logic:

Method: Stored Procedure with Dynamic SQL

This procedure takes the target table, source table, and key column(s) as input and generates/executes the MERGE statement.

### Key Considerations and Potential Enhancements:

Schema Alignment: This script assumes the source table (S) contains all columns listed in the target table (T) that are needed for the UPDATE and INSERT clauses. If the source might be missing columns, the generated SQL will fail. You might need more complex logic to handle schema drift (e.g., using COALESCE or only selecting common columns).

Error Handling: The example includes basic error handling with EXCEPTION WHEN ERROR. You can enhance this for more detailed logging or specific error management.

Permissions: The user or service account executing the stored procedure needs bigquery.tables.getData on the source table, bigquery.tables.updateData on the target table, and potentially bigquery.routines.get on the procedure itself, plus permissions to query INFORMATION_SCHEMA.

Quoting: The FORMAT function with %s handles basic table/column names. If your names contain special characters or are reserved words, you might need more robust quoting using backticks ()), although FORMAT often handles standard identifiers correctly. The use of \%s`` generally ensures proper quoting for table paths.

### Performance:

MERGE performance depends heavily on table size, partitioning/clustering (highly recommended on the key columns for both tables), and the complexity of the join.

Dynamic SQL itself has a small compilation overhead each time it's run via EXECUTE IMMEDIATE.

Column Exclusion: The options JSON parameter is included as a placeholder. You could extend the procedure to parse this JSON and exclude specific columns from the UPDATE SET or INSERT lists (e.g., audit columns like created_timestamp that should only be set on insert).

WHEN NOT MATCHED BY SOURCE: This script doesn't include a WHEN NOT MATCHED BY SOURCE THEN DELETE clause. You could add logic to optionally include this if needed.

Region: Ensure the INFORMATION_SCHEMA query targets the correct region if your datasets are not in the default location for your queries. You might need to specify the region explicitly (e.g., region-us.INFORMATION_SCHEMA.COLUMNS). The example tries to infer project/dataset, but be mindful of regionality.

This stored procedure provides a powerful and reusable way to perform standard MERGE operations across different tables in BigQuery without rewriting the core logic each time.

# calling stored procedure from airflow
Steps & Example DAG:

Import necessary modules: You'll need DAG, datetime, and BigQueryExecuteQueryOperator.

Define the DAG: Set up your DAG with its schedule, start date, etc.

Use BigQueryExecuteQueryOperator:

Set the task_id.

Set the sql parameter to the CALL statement for your stored procedure.

Set use_legacy_sql=False (Stored procedures use Standard SQL).

Specify your gcp_conn_id.

Optionally set the location if your dataset is not in the default location associated with your connection/project.

Explanation:

Configuration: Variables are set at the top for clarity (project ID, connection ID, dataset, procedure name, table details).

key_columns_sql_array: This line constructs the string representation of a BigQuery ARRAY<STRING>. It takes the Python list KEY_COLUMNS, puts single quotes around each element, joins them with commas, and wraps the result in square brackets (e.g., ['customer_id', 'product_id'] becomes ['customer_id', 'product_id']).

sql_call_statement: An f-string is used to build the CALL statement dynamically using the configuration variables.

The procedure name ({GCP_PROJECT_ID}.{SP_DATASET}.{SP_NAME}) is enclosed in backticks (``) as recommended for fully qualified BigQuery identifiers.

String arguments (TARGET_TABLE, SOURCE_TABLE) are enclosed in single quotes (' ').

The array argument (key_columns_sql_array) is inserted directly as it's already formatted correctly.

The JSON argument is passed as the SQL literal NULL. If you needed to pass an empty JSON object, you'd typically use PARSE_JSON('{}'). For more complex JSON, you might construct the JSON string carefully in Python first.

BigQueryExecuteQueryOperator:

sql: Receives the constructed CALL statement.

use_legacy_sql=False: Crucial for Standard SQL features like stored procedures.

location: Important if your procedure/datasets are not in the default BQ processing location for your project/connection.

gcp_conn_id: Tells Airflow which credentials to use.
