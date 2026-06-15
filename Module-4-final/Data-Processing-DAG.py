import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "ETL_SPARK_PROCESSING",
    schedule="@hourly",
    tags=["data-processing"],
    start_date=datetime.datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
) as dag:

    BashOperator(
        task_id="submit-dataproc-job",
        bash_command="""
        TOKEN=$(curl -sf -H "Metadata-Flavor: Google" \
          http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token \
          | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
        curl -sf -X POST \
          "https://dataproc.api.cloud.yandex.net/dataproc/v1/clusters/c9q3dtaime63ioh77235/jobs" \
          -H "Authorization: Bearer $TOKEN" \
          -H "Content-Type: application/json" \
          -d '{"name":"airflow-task2","pysparkJob":{"mainPythonFileUri":"s3a://petr-bondarev-module4-task1/scripts/process-csv-for-task2.py"}}' \
          | python3 -c "import sys,json; print('Job ID:', json.load(sys.stdin)['id'])"
        echo "DONE - job submitted"
        """,
    )