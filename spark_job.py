# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator \
        import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator \
        import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator \
        import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

#CLUSTER_ID = 'j-278EAV0LCH0W0'

def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    folder = kwargs['dag_run'].conf['folder']
    kwargs['ti'].xcom_push( key = 's3location', value = s3_location)
    kwargs['ti'].xcom_push( key = 'folder', value = folder)

JOB_FLOW_OVERRIDES = {
    'Name': 'de_bootcamp',
    'ReleaseLabel': 'emr-6.4.0',
    'Applications': [{'Name': 'Spark'},{'Name': 'Hadoop'}, {'Name': 'Hive'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Worker node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'Core',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'Ec2KeyName': 'wcdkey',
        'Placement': {
            'AvailabilityZone': 'us-east-1'
        },
        'Ec2SubnetId': 'subnet-2c545570',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'AutoScalingRole'='EMR_AutoScaling_DefaultRole'
}

SPARK_TEST_STEPS = [
    {
        'Name': 'datajob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode','cluster',
                '--num-executors','2',
                '--driver-memory','512m',
                '--executor-memory','3g',
                '--executor-cores','2',
                's3://qixuanma/spark-engine_2.12-0.0.1.jar',
                '-p','wcd-midterm',
                '-i','Json',
                '-o','parquet',
                #'-s','s3a://demo-wcd/banking.csv',
                '-f', "{{ task_instance.xcom_pull('parse_request', key='folder') }}",
                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3location') }}",
                '-d','s3://qixuanmadata/data/',
                '-m','overwrite'
            ]
        }
    }
]


dag = DAG(
    'emr_job_flow_manual_steps_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)


parse_request = PythonOperator(task_id='parse_request',
                             provide_context=True,
                             python_callable=retrieve_s3_file,
                             dag=dag
)

cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id=cluster_creator.output,
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id=cluster_creator.output,
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster', 
        job_flow_id=cluster_creator.output,
        dag=dag
)

parse_request>>cluster_creator>>step_adder>>step_checker>>cluster_remover

