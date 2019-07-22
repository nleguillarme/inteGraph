from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bkgb.modules.access.dump_import import DumpImport
from bkgb.modules.output.sparql_store import SPARQLStore
import yaml
from datetime import datetime, timedelta

config_file = "/home/leguilln/workspace/biodiv-kg-builder/bkgb/GloBiDumpImport.yml"
ymlfile = open(config_file, 'r')
cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

default_args = {
    'owner': 'leguilln',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 19),
    'email': ['nicolas.leguillarme@univ-grenoble-alpes.fr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('bkgb-pipeline', default_args=default_args, schedule_interval="@once")

t1 = PythonOperator(
    task_id='DumpImport',
    provide_context=False,
    python_callable=DumpImport(cfg).run,
    dag=dag,
)

t2 = PythonOperator(
    task_id='SPARQLStore',
    provide_context=False,
    python_callable=SPARQLStore(cfg).run,
    dag=dag,
)

t1 >> t2
