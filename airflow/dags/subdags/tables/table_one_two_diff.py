"""
table_one_two_diff.py
~~~~~~~~~~~~~~~~~~~~~~~~
"""
import sys
sys.path.append('/usr/local/airflow/dags')
#sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from class_variable_abstractions.table_abstraction_builder import table_class_builder
from dag_functions.table_functions import build_joined_table
from class_variable_abstractions.variable_creation import variable_creation

# Create new table here
#################################################################################################################
table_class_builder.create_new_table(name='one_and_two_day_diff_table',
                                     description='A template table',
                                     dialect='BigQuery',
                                     variable_components={'one_day_diff': 'LEFT OUTER JOIN',
                                                          'two_day_diff': 'LEFT OUTER JOIN'},
                                     schedule='@daily',
                                     depends=False,
                                     provide_context=False,
                                     op_kwargs={'table_instance': table_class_builder,
                                                'table_name': 'one_and_two_day_diff_table',
                                                'engine_name': 'ml-for-fundamental_will_airflow_db',
                                                'db_name': 'will_airflow_db',
                                                'commit_changes': True},
                                     dag_id='one_two_day_diff_table_dag')




def create_table_subdag(name: str, dag_id: str):
    table_namespace = table_class_builder.table_abstractions \
        .get(name) \
        .table_namespace

    # Create the dag instance
    dag = DAG(dag_id=dag_id, schedule_interval=table_namespace.get('schedule'), start_date=datetime(2020, 12, 12))

    # Task - Create table in SQL
    create_table_task = PythonOperator(task_id="create_table_{}".format(table_namespace.get(name)),
                                       python_callable=build_joined_table,
                                       provide_context=False,
                                       op_kwargs=table_namespace.get('op_kwargs'),
                                       dag=dag)

    return dag