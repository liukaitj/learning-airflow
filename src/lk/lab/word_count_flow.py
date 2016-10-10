# -*- coding: UTF-8 -*-
'''
Created on 2016年10月10日

@author: liukai
'''
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 10, 1),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'wordCountFlow', default_args=default_args, schedule_interval=timedelta(1))


'''
git pull
'''
git_pull_command = """
     cd /data/opt/airflow/resources/spark-algorithm/spark-algorithm
     if [ -d ".git" ]; then
         git pull origin
     else
         cd /data/opt/airflow/resources/spark-algorithm
         git clone https://github.lk/liukaitj/spark-algorithm.git
     fi
"""

opGitPull = BashOperator(
    task_id='opGitPull',
    bash_command=git_pull_command,
    dag=dag)

'''
maven build
'''
maven_command = """
     cd /data/opt/airflow/resources/spark-algorithm/spark-algorithm
     mvn clean package -Dmaven.test.skip=true
"""

opMvnBuild = BashOperator(
    task_id='opMvnBuild',
    bash_command=maven_command,
    dag=dag)


'''
check built spark jar
'''
def check_built_spark_jar(ds, **kwargs):
    if not os.path.isfile('/data/opt/airflow/resources/spark-algorithm/spark-algorithm/target/spark-algorithm-1.0-SNAPSHOT.jar'):
        raise IOError('spark-algorithm-1.0-SNAPSHOT.jar not exist!!')
    return 'fileCheckOk'

opCheckJar = PythonOperator(
    task_id='opCheckJar',
    provide_context=True,
    python_callable=check_built_spark_jar,
    trigger_rule="all_success",
    dag=dag)


'''
submit spark job
'''
spark_command = """
    spark-submit --master yarn-client \
     --name AccessLogAnalyzer \
     --class lk.lab.spark.AccessLogAnalyzer \
     /data/opt/airflow/resources/spark-algorithm/spark-algorithm/target/spark-algorithm-1.0-SNAPSHOT.jar {{params.p_file_path}}
"""

opSubmitSparkJob = BashOperator(
    task_id='opSubmitSparkJob',
    bash_command=spark_command,
    params={'p_file_path': 'hdfs://bds-hadoop-vm0:8020/user/sample/error.log'},
    dag=dag)



'''
set DAG
'''
opMvnBuild.set_upstream(opGitPull)
opCheckJar.set_upstream(opMvnBuild)
opSubmitSparkJob.set_upstream(opCheckJar)

if __name__ == "__main__":
    dag.cli()
