-- =====================================================
-- SEMANTIC MODEL EVALUATION - Complete Pipeline
-- Usage: snow sql -f sql/orchestrate_evaluation.sql --variable database=HOL2_DB_DEV --variable schema=HOL2_SCHEMA_DEV --variable warehouse=HOL2_WH_DEV --variable role=HOL2 --variable semantic_stage=SEMANTIC_MODEL --variable semantic_model_file=TIME_SERIES_REVENUE_V_VIEW --variable evaluation_model=llama3.1-70b --variable input_test_table=GENAI_UTILITIES.EVALUATION.SQL_SAMPLE_RESULTS
-- =====================================================

-- Set required parameters
SET DB_NAME = '<%database%>';
SET DB_SCHEMA = '<%schema%>'; 
SET DB_WAREHOUSE = '<%warehouse%>';
SET DB_ROLE = '<%role%>';
SET SEMANTIC_STAGE = '<%semantic_stage%>';
SET SEMANTIC_MODEL_FILE = '<%semantic_model_file%>';
SET EVALUATION_MODEL = '<%evaluation_model%>';
SET INPUT_TEST_TABLE = '<%input_test_table%>';

-- All evaluation logic in stored procedure

-- =====================================================
-- SETUP DATABASE CONTEXT FIRST
-- =====================================================

-- Setup Snowflake context
USE ROLE IDENTIFIER($DB_ROLE);
USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA IDENTIFIER($DB_SCHEMA);
USE WAREHOUSE IDENTIFIER($DB_WAREHOUSE);

-- =====================================================
-- AUTO-SETUP: Create Stored Procedures (if not exists)
-- =====================================================

-- Cortex Analyst runner for a single question
CREATE OR REPLACE PROCEDURE CORTEX_ANALYST_SQL(
    prompt STRING, 
    database STRING, 
    schema STRING, 
    stage STRING, 
    semantic_file STRING
)
RETURNS STRING
LANGUAGE PYTHON
PACKAGES = ('snowflake-snowpark-python')
RUNTIME_VERSION = '3.11'
HANDLER = 'process_message'
AS
$$
import _snowflake
import json

def send_message(messages, database, schema, stage, semantic_file):
    if 'yaml' in semantic_file or 'yml' in semantic_file:
        request_body = {
            "messages": messages,
            "semantic_model_file": f"@{database}.{schema}.{stage}/{semantic_file}",
        }
    else:
        request_body = {
            "messages": messages,
            "semantic_view": f"{database}.{schema}.{semantic_file}",
        }
    resp = _snowflake.send_snow_api_request("POST", f"/api/v2/cortex/analyst/message", {}, {}, request_body, {}, 30000)
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(f"Failed request with status {resp['status']}: {resp}")

def process_message(session, prompt, database, schema, stage, semantic_file):
    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
    response = send_message(messages, database, schema, stage, semantic_file)
    for item in response["message"]["content"]:
        if item["type"] == "sql":
            return item.get("statement", None)
    return None
$$;

-- Batch SQL generation procedure
CREATE OR REPLACE PROCEDURE GENERATE_SQL_ALL(
    input_table STRING,
    db_name STRING,
    schema_name STRING,
    stage_name STRING,
    config_file STRING,
    output_table STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_sql_all'
AS
$$
from snowflake.snowpark import Session

def generate_sql_all(session: Session, input_table: str, db_name: str, schema_name: str, stage_name: str, config_file: str, output_table: str) -> str:
    df = session.sql(f"SELECT question, expected_sql FROM {input_table}")
    result_rows = []
    
    for row in df.collect():
        question = row['QUESTION']
        expected_sql = row['EXPECTED_SQL']
        
        try:
            result = session.sql("CALL CORTEX_ANALYST_SQL(?, ?, ?, ?, ?)", params=[question, db_name, schema_name, stage_name, config_file]).collect()
            generated_sql = result[0][0] if result else None
        except Exception as e:
            generated_sql = f"[ERROR]: {str(e)}"
        
        result_rows.append((question, expected_sql, generated_sql, None, None))
    
    result_df = session.create_dataframe(result_rows, schema=["question", "expected_sql", "generated_sql", "evaluation_timestamp", "evaluation_run_id"])
    result_df.write.mode("overwrite").save_as_table(output_table)
    
    return f"Processed {len(result_rows)} rows into {output_table}"
$$;

-- Evaluation procedure
CREATE OR REPLACE PROCEDURE EVALUATE_ALL_SAMPLES(
    tbl_name STRING,
    model_name STRING DEFAULT 'llama3.1-70b'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

SQLAccuracy_prompt = """You are evaluating JSON data against ground truth JSON data.
The JSON data is the output of a SQL query generated to answer a user question.
You are to determine if the provided JSON data matches the ground truth JSON data and answers the user question.
The Inference JSON does not have to match the Ground Truth JSON perfectly but should contain the correct answer as denoted by the Ground Truth JSON.
Your answer should be either "True" or "False".
Answer "True" if you believe the Inference JSON data reflects the Ground Truth JSON data given the user question.
Otherwise, answer "False".
[User Question]
{question}

[The Start of the Inference JSON Data]
{inference_data}
[The End of the Inference JSON Data]

[The Start of the Ground Truth Data]
{expected_data}
[The End of the Ground Truth Data]
"""

def return_sql_result(session: Session, sql: str) -> str:
    try:
        result = (session.sql(sql.replace(";", "")).limit(100).select(F.to_varchar(F.array_agg(F.object_construct("*")))))
        return result.collect_nowait().result()[0][0]
    except Exception as e:
       return f"Error: {e}"

def run_async_sql_complete(session: Session, model: str, prompt: str) -> str:
    prompt = prompt.replace("'", "\\'")
    query = f"SELECT TRIM(snowflake.cortex.complete('{model}', '{prompt}'))"
    return session.sql(query).collect_nowait().result()[0][0]

def main(session: Session, tbl_name: str, model_name: str) -> str:
    if model_name is None:
        model_name = "llama3.1-70b"

    # Update evaluation_timestamp for all rows at start of evaluation
    session.sql(f"UPDATE {tbl_name} SET evaluation_timestamp = CURRENT_TIMESTAMP() WHERE evaluation_timestamp IS NULL").collect()

    df = session.table(tbl_name)
    rows = df.collect()
    
    total = len(rows)
    true_count = 0
    return_msg = ""

    for i, row in enumerate(rows, 1):
        question = row['QUESTION']
        generated_sql = row['GENERATED_SQL']
        expected_sql = row['EXPECTED_SQL']

        inference_data = return_sql_result(session, generated_sql)
        expected_data = return_sql_result(session, expected_sql)

        if inference_data.startswith("Error") or expected_data.startswith("Error"):
            return_msg += (f"[{i}/{total}] Skipped due to SQL error \n")
            continue

        fstrings = {"question": question, "inference_data": inference_data, "expected_data": expected_data}
        prompt = SQLAccuracy_prompt.format(**fstrings)

        try:
            result = run_async_sql_complete(session, model_name, prompt)
            if result.strip().lower() == "true":
                true_count += 1
            return_msg += (f"[{i}/{total}] Result: {result}\n")
        except Exception as e:
            return_msg += (f"[{i}/{total}] Cortex Error: {e}\n")

    accuracy = (true_count / total) * 100 if total > 0 else 0
    return_msg += (f"Evaluation complete: {true_count}/{total} correct ({accuracy:.2f}%)")
    return return_msg
$$;

-- Complete evaluation orchestration procedure
CREATE OR REPLACE PROCEDURE RUN_COMPLETE_EVALUATION(
    input_table STRING,
    db_name STRING,
    schema_name STRING,
    stage_name STRING,
    semantic_file STRING,
    output_table STRING,
    eval_model STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Generate unique run ID
    LET run_id STRING := CONCAT('eval_', TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY_MM_DD_HH24_MI_SS'));
    LET sq STRING := CHR(39);  -- Single quote character for dynamic SQL
    
    -- Create evaluation table
    LET create_sql STRING := 'CREATE OR REPLACE TABLE ' || :output_table || ' (
        question STRING,
        expected_sql STRING,
        generated_sql STRING,
        evaluation_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
        evaluation_run_id STRING DEFAULT NULL
    )';
    EXECUTE IMMEDIATE :create_sql;
    
    -- Generate SQL using Cortex Analyst  
    CALL GENERATE_SQL_ALL(:input_table, :db_name, :schema_name, :stage_name, :semantic_file, :output_table);
    
    -- Update run ID
    LET update_sql STRING := 'UPDATE ' || :output_table || ' SET evaluation_run_id = ' || :sq || :run_id || :sq || ' WHERE evaluation_run_id IS NULL';
    EXECUTE IMMEDIATE :update_sql;
    
    -- Evaluate accuracy and capture results
    LET eval_results STRING;
    CALL EVALUATE_ALL_SAMPLES(:output_table, :eval_model) INTO :eval_results;
    
    -- Show results using CHR(39) for single quotes to avoid escaping issues
    LET results_sql STRING := 'SELECT ' || :sq || :run_id || :sq || ' as RUN_ID, ' ||
                              :sq || :db_name || :sq || ' as DATABASE, ' ||
                              :sq || :schema_name || :sq || ' as SCHEMA, ' ||
                              'COUNT(*) as TOTAL_QUESTIONS, ' ||
                              'SUM(CASE WHEN generated_sql NOT LIKE ' || :sq || '[ERROR]%' || :sq || ' THEN 1 ELSE 0 END) as SUCCESSFUL_GENERATIONS, ' ||
                              'ROUND((SUM(CASE WHEN generated_sql NOT LIKE ' || :sq || '[ERROR]%' || :sq || ' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as SUCCESS_RATE_PCT ' ||
                              'FROM ' || :output_table || ' WHERE evaluation_run_id = ' || :sq || :run_id || :sq;
    EXECUTE IMMEDIATE :results_sql;
    
    -- Display evaluation results with accuracy
    SELECT 'EVALUATION RESULTS:' as MESSAGE, :eval_results as DETAILS;
    
    RETURN :eval_results;
END;
$$;

-- Core stored procedures created/updated

-- =====================================================
-- EXECUTION
-- =====================================================

-- Set output table name  
SET EVAL_RESULTS_TABLE = 'EVAL_RESULTS';
SET EVAL_RESULTS_TABLE_FULL = $DB_NAME || '.' || $DB_SCHEMA || '.' || $EVAL_RESULTS_TABLE;

-- Ready for evaluation

-- =====================================================
-- RUN EVALUATION
-- =====================================================

-- Run complete evaluation pipeline (results displayed within procedure)
CALL RUN_COMPLETE_EVALUATION($INPUT_TEST_TABLE, $DB_NAME, $DB_SCHEMA, $SEMANTIC_STAGE, $SEMANTIC_MODEL_FILE, $EVAL_RESULTS_TABLE_FULL, $EVALUATION_MODEL);

-- Evaluation complete
