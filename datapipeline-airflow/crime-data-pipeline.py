from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('police_crime_performance_pipeline', default_args=default_args, schedule_interval=timedelta(days=1))

def create_dag_task(task_id, bash_command):
    return BashOperator(task_id = task_id, bash_command = bash_command, dag = dag)

def create_hive_dag_task(task_id, bash_command, db_schema = 'datawarehouse'):
    script = 'hive -f ' + bash_command
    return SSHOperator(task_id=task_id, command=script, ssh_conn_id='ssh_default',schema=db_schema,dag=dag)

spark_etl_home = "/home/mahpara/etl/sparklyr/"
hive_task_home = "/home/maria_dev/data-warehouse/"
source_home = "/home/maria_dev/source/"

data_ingestion = create_hive_dag_task("data_ingestion_task", source_home + "hive-ingestion.hql", "crime")

crime_and_outcome_etl = create_dag_task("crime_outcome_spark_etl_task", "Rscript " + spark_etl_home + "crime_detail_outcome.R")
police_arrest_etl = create_dag_task("police_arrest_spark_etl_task", "Rscript " + spark_etl_home + "police_arrest.R")
crime_status_etl = create_dag_task("crime_status_spark_etl", "Rscript " + spark_etl_home + "crime_status.R")
police_leaver_and_absent_etl = create_dag_task("police_leaver_absence_spark_etl_task", "Rscript " + spark_etl_home + "police_leaver_absent.R")
region_etl = create_dag_task("region_spark_etl_task", "Rscript " + spark_etl_home + "region.R")

data_validation = create_dag_task("data_validation_spark_task", spark_etl_home + "data_validation.R")

data_mart_crime_outcome = create_hive_dag_task("data_mart_crime_outcome", hive_task_home + "outcome-data-mart.hql")
data_mart_police_crime_type = create_hive_dag_task("data_mart_police_crime_type", hive_task_home + "police-crime-type-data-mart.hql")
data_mart_crime_type = create_hive_dag_task("data_mart_crime_type", hive_task_home + "crime-type-data-mart.hql")
data_mart_arrest = create_hive_dag_task("data_mart_arrest", hive_task_home + "arrest-data-mart.hql")
data_mart_leaver_absent  = create_hive_dag_task("data_mart_leaver_absent", hive_task_home + "leaver-absent-data-mart.hql")
data_mart_crime_status  = create_hive_dag_task("data_mart_crime_status", hive_task_home + "crime-status-data-mart.hql")

data_warehouse_creation = create_hive_dag_task("datawarehouse", hive_task_home + "full-datawarehouse.hql", "full-datawarehouse")

# Set task dependencies
data_ingestion >> [crime_and_outcome_etl, police_arrest_etl, crime_status_etl, police_leaver_and_absent_etl, region_etl ] >> data_validation \
    >> [data_mart_crime_outcome, data_mart_arrest, data_mart_crime_status, data_mart_leaver_absent, data_mart_crime_type, data_mart_police_crime_type ] \
    >> data_warehouse_creation  