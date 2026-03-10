/* 
 ================================
 CREATE SP: dbt(args)
   führt dbt mit beliebigen Argumenten aus, speichert die Artefakte in einem Stage-Pfad ab und gibt eine Tabelle mit den Resultaten zurück
 ________________________________
*/

CREATE OR REPLACE PROCEDURE dbt(args VARCHAR DEFAULT 'debug')
    RETURNS TABLE(
        ARGS VARCHAR,
        SUCCESS BOOLEAN,
        EXCEPTION VARCHAR,
        STD_OUT VARCHAR,
        START_TIME VARCHAR,
        END_TIME VARCHAR,
        QUERY_ID VARCHAR,
        STAGE_FOLDER VARCHAR,
        STAGE_URL VARCHAR,
        RUN_RESULTS VARCHAR,
        QUERY_HISTORY_URL VARCHAR,
        INVOCATION_ID VARCHAR,
        RETURN_SUMMARY VARCHAR
    )
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.12
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'main'
    COMMENT = 'executes dbt with the provided arguments, stores the artifacts in a stage and returns a table with the results'
    EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as sp
import zipfile
import io
import re

from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StringType

from datetime import datetime, timedelta, timezone
from urllib.parse import quote


STAGE = "dbt_run_results"
ARTIFACTS = f"{STAGE}/artifacts"
RESULTS = f"{STAGE}/results"


def get_current_account_details(session: sp.Session):
    """
    Retrieves the current account details such as organization name, account name,
    current database, and current schema from the Snowflake session.
    """
    org_name: str = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
    acc_name: str = session.sql("SELECT CURRENT_ACCOUNT_NAME()").collect()[0][0]
    current_db: str = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    current_schema: str = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    current_warehouse: str = session.sql("SELECT CURRENT_WAREHOUSE()").collect()[0][0]

    return org_name, acc_name, current_db, current_schema, current_warehouse


def get_return_summary(stdout: str) -> str:
    """
    Extracts the summary of the dbt run from the stdout.
    """

    # Example summary line:
    # PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
    pattern = [
      r"PASS\=[\d]{1,}\sWARN\=[\d]{1,}\sERROR\=[\d]{1,}\sSKIP\=[\d]{1,}\sTOTAL\=[\d]{1,}",
      r"Nothing to do\. Try checking your model configs and model specification args"
    ]
    ret_regex_search = "|".join(pattern)
    ret_match = re.search(ret_regex_search, stdout)
    if ret_match:
        return ret_match.group(0)

    return "No execution summary found in stdout" 


def copy_artifacts_into_internal_stage(session: sp.Session, query_id: str) -> str:
    """
    Copies the dbt artifacts from the location provided by the
    SYSTEM$LOCATE_DBT_ARTIFACTS function into an internal stage.
    Returns the path to the folder in the internal stage.
    """

    # Get the path to the artifacts using the new SYSTEM$LOCATE_DBT_ARTIFACTS function
    artifact_path: str = session.sql(
        f"SELECT SYSTEM$LOCATE_DBT_ARTIFACTS('{query_id}')"
    ).collect()[0][0]

    # Copy the artifacts into the internal stage
    stage_folder: str = f"{ARTIFACTS}/query-id_{query_id}/"
    _ = session.sql(f"COPY FILES INTO @{stage_folder} FROM '{artifact_path}'").collect()
    # The archive contains:
    # {ARTIFACTS}/query-id_{query_id}/dbt_artifacts.zip
    # {ARTIFACTS}/query-id_{query_id}/logs/dbt.log
    # {ARTIFACTS}/query-id_{query_id}/target/manifest.json
    # {ARTIFACTS}/query-id_{query_id}/target/semantic_manifest.json

    return stage_folder


def generate_query_history_link(
    invocation_id: str, start_dt: datetime, end_dt: datetime
) -> str:
    """
    Generates a link to the Snowflake Query History page filtered by the invocation ID
    and the provided start and end datetime.
    The link format is based on the Snowflake web interface as of 2025.
    """

    # Format datetimes to the required string format
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Generate the link
    snowsight_link = (
        f"https://app.snowflake.com/{org_name}/{acc_name}/#/compute/history/queries"
        "?user=ALL"
        "&columns=sqlText%2CqueryId%2Cstatus%2CtotalDuration%2CstartTime%2CqueryTag"
        "&preset=CUSTOM_PRESET"
        f"&startDate={quote(start_str)}"
        f"&endDate={quote(end_str)}"
        "&page=history%2Fqueries"
        "&user_task="
        f"&wh={current_warehouse}"
        f"&query_tag=%7C%20{invocation_id}%20%7C"
    )

    return snowsight_link


def extract_run_results_from_dbt_artifacts(session: sp.Session, query_id: str) -> str:
    """
    Extracts the run_results.json file from a dbt_artifacts.zip stored in a stage
    and uploads it to the results folder.
    """

    # Download ZIP file from stage as stream and read its content
    zip_stream = session.file.get_stream(
        f"@{ARTIFACTS}/query-id_{query_id}/dbt_artifacts.zip", decompress=False
    )
    with zipfile.ZipFile(zip_stream) as zf:
        extracted_bytes = zf.read("target/run_results.json")

    # Define target path for upload and upload extracted file to stage
    run_results = f"@{RESULTS}/query-id_{query_id}_run-results.json"
    session.file.put_stream(
        io.BytesIO(extracted_bytes), run_results, overwrite=True, auto_compress=False
    )
    return run_results


def get_stage_link(stage_folder: str) -> str:
    """
    Generates a link to the Snowflake stage where the dbt artifacts are stored.
    """

    # generate link
    stage_url = (
        f"https://app.snowflake.com/{org_name}/{acc_name}/"
        f"#/data/databases/{current_db}/schemas/{current_schema}/"
        f"stage/{STAGE.upper()}?path={stage_folder.replace(STAGE, '').rstrip('/')}"
    )
    return stage_url


def get_invocation_id_from_run_results(session: sp.Session, run_results: str) -> str:
    """
    Extracts the invocation ID from the run_results.json file stored in the stage.
    """

    # read run_results.json from stage into DataFrame and extract invocation_id
    df = session.read.json(run_results)
    invocation_id_row = df.select(
        col("$1")
        .getField("metadata")
        .getField("invocation_id")
        .cast(StringType())
        .alias("invocation_id")
    ).first()

    invocation_id: str = invocation_id_row[0]

    return invocation_id


def main(session: sp.Session, args: str):

    global org_name, acc_name, current_db, current_schema, current_warehouse
    org_name, acc_name, current_db, current_schema, current_warehouse = (
        get_current_account_details(session)
    )

    # _____________________________________________________________________________________________
    # Execute the DBT project with the provided arguments
    # and capture the output as well as any exceptions and time of execution

    escaped_args = args.replace("'", "''")
    start_dt = datetime.now(timezone.utc)
    try:
        for row in session.sql(
            f"EXECUTE DBT PROJECT enterprise_data_hub args='{escaped_args}'"
        ).collect():
            success, exception, stdout = row

    except sp.exceptions.SnowparkSQLException as sqe:
        success = "False"
        exception = sqe.message
        stdout = None
    end_dt = datetime.now(timezone.utc)

    # _____________________________________________________________________________________________
    # After execution, retrieve the last query ID, copy artifacts into internal stage
    # and extract run_results.json from dbt_artifacts.zip

    last_query_id: str = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]

    if not stdout:
        # Returns the last 1000 lines of the dbt.log file. For full logs, download the archive zip.
        stdout_retrieve_query = f"select system$get_dbt_log('{last_query_id}')"
        stdout = session.sql(stdout_retrieve_query).collect()[0][0]

    # copy artifacts into internal stage and extract run_results.json
    stage_folder: str = copy_artifacts_into_internal_stage(session, last_query_id)
    run_results: str = extract_run_results_from_dbt_artifacts(session, last_query_id)

    invocation_id: str = get_invocation_id_from_run_results(session, run_results)
    print(f"Invocation ID: {invocation_id}")

    # get summary of execution results
    return_summary: str = get_return_summary(stdout)

    # _____________________________________________________________________________________________
    # prepare links for snowsight to access stage and query history

    stage_url: str = get_stage_link(stage_folder)
    query_history_url: str = generate_query_history_link(
        invocation_id, start_dt, end_dt
    )

    # _____________________________________________________________________________________________
    # Return a single row dataframe with all relevant information about the execution
    ret_data = {
        "ARGS": str(args),
        "SUCCESS": str(success) == "True",
        "EXCEPTION": exception,
        "STD_OUT": str(stdout),
        "START_TIME": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "END_TIME": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "QUERY_ID": str(last_query_id),
        "STAGE_FOLDER": str(stage_folder),
        "STAGE_URL": str(stage_url),
        "RUN_RESULTS": str(run_results),
        "QUERY_HISTORY_URL": str(query_history_url),
        "INVOCATION_ID": str(invocation_id),
        "RETURN_SUMMARY": str(return_summary),
    }
    df = session.create_dataframe([ret_data])
    return df
$$;