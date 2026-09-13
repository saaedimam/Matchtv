"""Airflow DAG: IPTV Export -- runs the exporter script on a schedule."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='iptv_export',
    default_args=default_args,
    description='Export IPTV repos playlists',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    repos = Variable.get('IPTV_REPOS', default_var='iptv-org/iptv,Guovin/iptv-api,HerbertHe/iptv-sources,yuanzl77/IPTV,DangJin/awesome-iptv')
    output_dir = Variable.get('IPTV_OUTPUT_DIR', default_var='/tmp/iptv_exports')
    max_workers = Variable.get('IPTV_MAX_WORKERS', default_var='6')
    github_token = Variable.get('GITHUB_TOKEN', default_var='')

    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command=(
            'python -m pip install --upgrade pip && '
            'pip install -r /opt/airflow/dags/tools/requirements.txt'
        ),
    )

    run_export = BashOperator(
        task_id='run_export_script',
        bash_command=(
            f"export GITHUB_TOKEN='{github_token}' && "
            f"python /opt/airflow/dags/tools/export_agent.py --repos \"{repos}\" "
            f"--output-dir {output_dir} --max-workers {max_workers}"
        ),
    )

    install_deps >> run_export
