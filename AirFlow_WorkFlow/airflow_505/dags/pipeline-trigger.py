
import json
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

logger = logging.getLogger(__name__)

def validate_input_file(input_path, file_extension=""):
    """Fail early if the input file does not exist."""
    input_path = (input_path).strip()
    print(f"Validating input file: {input_path}, {os.path.isfile(input_path)}, {os.path.isdir(input_path)}")
    if not input_path:
        raise AirflowException("inputPath is missing in dag_run.conf")

    if os.path.isfile(input_path):                       
        logger.info("Input file found: %s", input_path)
    elif os.path.isdir(input_path):                     
        found = [f for f in os.listdir(input_path) if f.endswith(file_extension or "")]
        if not found:
            raise AirflowException(
                f"No '{file_extension}' files inside folder {input_path}"
            )
        logger.info("Found %d file(s) in %s", len(found), input_path)
    else:
        raise AirflowException(f"Input file not found: {input_path}")


def read_control_file_count(control_file):
    """Read the recordCount the Beam pipeline wrote into the control file.

    Two formats are supported:
        {"recordCount": 10, ...}   (JSON)
        recordCount=10             (key=value)
    """
    if not os.path.isfile(control_file):
        raise AirflowException(f"Control file not found: {control_file}")
    content = open(control_file).read()

    try:                                                
        return int(json.loads(content)["recordCount"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        pass

    for line in content.splitlines():                    
        key, _, value = line.partition("=")
        if key.strip().lower() in ("recordcount", "record_count"):
            return int(value.strip())

    raise AirflowException(f"recordCount not found in control file: {control_file}")


def verify_record_count(control_file, execution_id):
    """Check: recordCount in the control file == COUNT(*) in the database."""
    execution_id = (execution_id or "").strip()
    if not execution_id:
        raise AirflowException("executionId is missing in dag_run.conf")

    expected = read_control_file_count(control_file)

    db_url = os.environ.get("INGESTION_DB_URL", "").replace("jdbc:", "", 1)  # allow jdbc:
    if not db_url:
        raise AirflowException(
            "INGESTION_DB_URL is missing - check the .env file / docker-compose"
        )

    url = make_url(db_url)
    username = os.environ.get("INGESTION_DB_USERNAME", "")
    if username:
        url = url.set(username=username,
                      password=os.environ.get("INGESTION_DB_PASSWORD", ""))

    engine = create_engine(url)
    with engine.connect() as connection:
        actual = connection.execute(
            text("SELECT COUNT(*) FROM employee_warehouse WHERE execution_id = :id"),
            {"id": execution_id},
        ).scalar()

    if expected != actual:
        raise AirflowException(
            f"Record count mismatch for executionId '{execution_id}': "
            f"control file says {expected}, database has {actual}"
        )
    logger.info("Record count OK for '%s': %d == %d", execution_id, expected, actual)


default_args = {
    "owner": "Rachit",
    "retries": 0
}

with DAG(
    dag_id="pipeline-trigger",
    default_args=default_args,
    description="Parse and ingest a source file with Apache Beam and verify record counts",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    validate_input_file_task = PythonOperator(
        task_id="validate_input_file",
        python_callable=validate_input_file,
        op_kwargs={
            "input_path": "{{ (dag_run.conf or {}).get('inputPath', '') }}",
            "file_extension": "{{ (dag_run.conf or {}).get('fileExtension', '') }}",
        },
    )

    run_beam_ingestion = BashOperator(
        task_id="run_beam_ingestion",
        bash_command=(
            'java -jar /opt/airflow/jar/beam-ingestion-pipeline.jar '
            '--inputPath="$INPUT_PATH" '
            '--fileExtension="$FILE_EXTENSION" '
            '--executionId="$EXECUTION_ID" '
            '--errorLogPath="$ERROR_LOG_PATH" '
        ),
        env={
            "INPUT_PATH": "{{ dag_run.conf['inputPath'] }}",
            "FILE_EXTENSION": "{{ dag_run.conf['fileExtension'] }}",
            "EXECUTION_ID": "{{ dag_run.conf['executionId'] }}",
            "ERROR_LOG_PATH": "{{ dag_run.conf['errorLogPath'] }}"
        },
        append_env=True,
    )

    verify_record_count_task = PythonOperator(
        task_id="verify_record_count",
        python_callable=verify_record_count,
        op_kwargs={
            "control_file": "{{ (dag_run.conf or {}).get('controlFile', '') }}",
            "execution_id": "{{ (dag_run.conf or {}).get('executionId', '') }}",
        },
    )

    validate_input_file_task >> run_beam_ingestion >> verify_record_count_task
