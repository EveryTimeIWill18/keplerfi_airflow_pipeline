"""
two_day_diff.py
~~~~~~~~~~~~~~~
"""
import sys
sys.path.append('/usr/local/airflow/dags')
# sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
from airflow import DAG
from datetime import datetime, timedelta
from class_variable_abstractions.variable_abstraction_builder import variable_class_builder
from airflow.operators.python_operator import PythonOperator
from class_variable_abstractions.variable_creation import variable_creation
from dag_functions.variable_functions import create_var_table, insert_data


# Create new variable here
#################################################################################################################
variable_class_builder.create_new_variable(var_name='two_day_diff',
                                           data_source='Shardar',
                                           data_destination='BigQuery',
                                           description='One day closing price difference',
                                           query_logic="""select a.date, a.ticker,  a.close/b.close-1 as value
                                                          from
                                                            (select ticker, date, close from will_airflow_db.price_data
                                                          where ticker = 'FB'
                                                          and date = '<DATEID>')  a
                                                          left outer join
                                                            (select ticker, date, close from will_airflow_db.price_data
                                                          where ticker = 'FB' and
                                                            date = DATE_SUB('<DATEID>', INTERVAL 2 DAY)) b
                                                            on a.ticker = b.ticker
                                           """,
                                           data_provenance='NA',
                                           data_quality='Good',
                                           variable_type='Float',
                                           schedule='@daily',
                                           start_date=datetime(2020, 12, 12),
                                           provide_context={'create_var_table': False,
                                                            'insert_date': True},
                                           op_kwargs={
                                               'create_var_table':
                                                   {'dataset_name': 'will_airflow_db',
                                                    'project_name': 'ml-for-fundamental',
                                                    # Variable name to be update here
                                                    'table_name': 'two_day_diff',
                                                    'date': 'DATE',
                                                    'ticker': 'STRING',
                                                    'value': 'FLOAT64'},
                                               'insert_data':
                                                   {'dataset_name': 'will_airflow_db',
                                                    'project_name': 'ml-for-fundamental',
                                                    'table_name': 'will_airflow_db.two_day_diff',
                                                    'module_dir': variable_class_builder.MODULE_DIR,
                                                    'module_name': 'two_day_diff',
                                                    'dest_dir': variable_class_builder.DEST_DIR,
                                                    'commit_changes': True}
                                           }
                                          ,
                                           dag_id='one_day_diff_dag')

# Create Sub-dag here
#################################################################################################################
def create_variable_subdag(var_name: str, dag_id: str):
    """Creates the variable sub-dag"""

    # Select the variable
    variable_namespace = variable_class_builder.variable_abstractions.get(var_name)

    # Create a dag instance
    dag = DAG(dag_id=dag_id, schedule_interval=variable_namespace.__dict__['schedule'],
              start_date=variable_namespace.__dict__["start_date"])

    # Create the Airflow PythonOperator tasks for the variable

    # Task - Create variable table in SQL
    create_variable_table = PythonOperator(task_id="create_{}".format(var_name),
                                           python_callable=create_var_table,
                                           provide_context=False,
                                           op_kwargs=variable_namespace \
                                                .__dict__['op_kwargs']  \
                                                .get('create_var_table'),
                                           dag=dag)

    # Task - Insert data into the newly created SQL table
    insert_into_variable_table = PythonOperator(task_id="insert_into_{}".format(var_name),
                                                python_callable=insert_data,
                                                provide_context=True,
                                                op_kwargs=variable_namespace \
                                                    .__dict__['op_kwargs']   \
                                                    .get('insert_data'),
                                                dag=dag)

    # function must return the dag in order for the parent dag to recognize the sub-dag
    return dag
#################################################################################################################
# END