CREATE OR REPLACE PROCEDURE `your_project.your_dataset.usp_generic_merge`(
  target_table_name STRING,    -- Format: project.dataset.table
  source_table_name STRING,    -- Format: project.dataset.table or just dataset.table if in same project
  key_columns ARRAY<STRING>,   -- Array of column names forming the unique key
  options JSON                 -- Optional: For future extensions, e.g., exclude columns
)
BEGIN
  /*
  Generic MERGE procedure for BigQuery.
  Merges data from a source table into a target table based on key columns.
  It automatically detects columns (excluding keys for UPDATE) and builds the statement.

  Args:
    target_table_name: Full path of the target table (e.g., 'myproject.mydataset.mytable').
    source_table_name: Full path of the source table (e.g., 'myproject.mydataset.mysource').
    key_columns: An array of strings representing the primary/unique key column(s) used for joining.
    options: JSON object for future options (currently unused, pass NULL or empty JSON '{}').
  */

  -- Declare variables to hold parts of the dynamic SQL
  DECLARE merge_sql STRING;
  DECLARE join_condition STRING;
  DECLARE update_set_list STRING;
  DECLARE insert_col_list STRING;
  DECLARE insert_val_list STRING;
  DECLARE target_cols ARRAY<STRUCT<column_name STRING, data_type STRING>>;

  -- Input validation (basic)
  IF target_table_name IS NULL OR source_table_name IS NULL OR ARRAY_LENGTH(key_columns) = 0 THEN
    RAISE USING MESSAGE = 'Target table, source table, and at least one key column must be provided.';
  END IF;

  -- Extract schema/dataset/table names for INFORMATION_SCHEMA query
  DECLARE target_project_id, target_dataset_id, target_table_id STRING;
  DECLARE source_project_id, source_dataset_id, source_table_id STRING;

  SET (target_project_id, target_dataset_id, target_table_id) = (
    SELECT AS STRUCT SPLIT(target_table_name, '.')[SAFE_OFFSET(0)], SPLIT(target_table_name, '.')[SAFE_OFFSET(1)], SPLIT(target_table_name, '.')[SAFE_OFFSET(2)]
  );
   -- Assume source is in the same project if not specified
  SET (source_project_id, source_dataset_id, source_table_id) = (
    SELECT AS STRUCT
        IFNULL(SPLIT(source_table_name, '.')[SAFE_OFFSET(0)], @@project_id),
        SPLIT(source_table_name, '.')[SAFE_OFFSET(1)],
        SPLIT(source_table_name, '.')[SAFE_OFFSET(2)]
  );


  -- 1. Get Target Table Columns (using the target table as the reference for columns to update/insert)
  -- Ensure you query the correct region's INFORMATION_SCHEMA if necessary
  EXECUTE IMMEDIATE FORMAT("""
    SELECT ARRAY_AGG(STRUCT(column_name, data_type))
    FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '%s'
  """, target_project_id, target_dataset_id, target_table_id)
  INTO target_cols;

  IF ARRAY_LENGTH(target_cols) = 0 THEN
     RAISE USING MESSAGE = FORMAT("Could not find columns for target table: %s", target_table_name);
  END IF;

  -- 2. Construct JOIN condition (`ON T.key1 = S.key1 AND T.key2 = S.key2 ...`)
  SET join_condition = (
    SELECT STRING_AGG(FORMAT('T.%s = S.%s', k, k), ' AND ')
    FROM UNNEST(key_columns) AS k
  );

  -- 3. Construct UPDATE SET list (`SET T.col1 = S.col1, T.col2 = S.col2 ...`)
  --    Exclude key columns from the SET list
  SET update_set_list = (
    SELECT STRING_AGG(FORMAT('T.%s = S.%s', column_name, column_name), ', ')
    FROM UNNEST(target_cols)
    WHERE column_name NOT IN UNNEST(key_columns) -- Exclude key columns
  );

  -- Handle case where ALL columns are keys (unlikely for MERGE update, but possible)
   IF update_set_list IS NULL OR update_set_list = '' THEN
    -- Option 1: Raise an error
    -- RAISE USING MESSAGE = 'No columns available to update (all columns might be keys).';
    -- Option 2: Create a dummy update (less ideal, might depend on use case)
    -- SET update_set_list = CONCAT('T.', key_columns[OFFSET(0)], ' = S.', key_columns[OFFSET(0)]); -- Update a key with itself (no-op)
    -- Option 3: Skip the WHEN MATCHED clause (Requires modifying the main MERGE construction logic)
    -- For this example, we'll assume there's always something to update or the user wants an error.
     RAISE USING MESSAGE = 'No non-key columns found to build the UPDATE SET clause.';
   END IF;


  -- 4. Construct INSERT column list (`(col1, col2, key1, key2)`)
  SET insert_col_list = (
    SELECT CONCAT('(', STRING_AGG(column_name, ', '), ')')
    FROM UNNEST(target_cols)
  );

  -- 5. Construct INSERT values list (`VALUES (S.col1, S.col2, S.key1, S.key2)`)
  SET insert_val_list = (
    SELECT CONCAT('VALUES (', STRING_AGG(FORMAT('S.%s', column_name), ', '), ')')
    FROM UNNEST(target_cols)
  );

  -- 6. Assemble the final MERGE statement
  SET merge_sql = FORMAT("""
    MERGE INTO `%s` AS T
    USING `%s` AS S
    ON %s
    WHEN MATCHED THEN
      UPDATE SET %s
    WHEN NOT MATCHED THEN
      INSERT %s %s;
  """,
    target_table_name,
    source_table_name,
    join_condition,
    update_set_list,
    insert_col_list,
    insert_val_list
  );

  -- 7. Execute the dynamic SQL
  EXECUTE IMMEDIATE merge_sql;

  -- Optional: Log success
  SELECT FORMAT("Successfully merged data from %s into %s", source_table_name, target_table_name) AS status;

EXCEPTION WHEN ERROR THEN
  -- Log or re-raise the error
  SELECT
    FORMAT("Error merging data from %s into %s. SQL: %s. Error: %s",
            source_table_name, target_table_name, IFNULL(merge_sql, 'SQL construction failed'), @@error.message) AS error_message,
    @@error.stack_trace AS stack_trace;
  RAISE; -- Re-raise the original error
END;
