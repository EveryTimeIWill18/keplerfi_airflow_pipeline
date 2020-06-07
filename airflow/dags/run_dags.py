"""
run_dags.py
~~~~~~~~~~~
This module runs the sub-dags for variable and table creation

NOTE:
    In airflow.cfg, line 69, if we switch from SequentialExecutor to LocalExecutor to test parallel run
    we need to use a different database, other than that of sqlite
"""
import os
import sys
import inspect
import pathlib
import importlib
from pprint import pprint
from airflow import DAG
from collections import deque
from datetime import datetime, timedelta
from typing import List, Dict, Any
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
#sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
sys.path.append('/usr/local/airflow/dags')
from class_variable_abstractions.variable_abstraction_builder import variable_class_builder
from class_variable_abstractions.table_abstraction_builder import table_class_builder


# TODO: START/ remove this commented out code block
####################################################################################################
# # Create a list of the imported sub-dag modules
# imported_sub_dag_modules: List[Any] = []
#
# # Grab the directory for the sub-dag variables
# sub_dag_variables_dir = os.path.join(Path(__file__).parent, 'subdags', 'variables')
#
# # Grab the directory for the sub-dag tables
# sub_dag_tables_dir = os.path.join(Path(__file__).parent, 'subdags', 'tables')
#
# # Append to sys.path
# sys.path.append(sub_dag_variables_dir)
# sys.path.append(sub_dag_tables_dir)
#
# # Grab only the sub-dags, excluding `__init__.py` and `__pycache__`
# sub_dag_variables = list(filter(lambda y: '__' not in str(y), os.listdir(sub_dag_variables_dir)))
#
# # Grab only the sub-dags, excluding `__init__.py` and `__pycache__`
# sub_dag_tables = list(filter(lambda y: '__' not in str(y), os.listdir(sub_dag_tables_dir)))
#
#
# # Grab the variable modules, excluding `__init__.py` and `__pycache__`
# sub_dag_variable_files = list(map(lambda x: os.path.join(sub_dag_variables_dir, x),
#                          list(filter(lambda y: '__' not in str(y),
#                                      os.listdir(sub_dag_variables_dir)))))
#
# # Grab the table modules, excluding `__init__.py` and `__pycache__`
# sub_dag_table_files = list(map(lambda x: os.path.join(sub_dag_tables_dir, x),
#                          list(filter(lambda y: '__' not in str(y),
#                                      os.listdir(sub_dag_tables_dir)))))
#
# # Append all sub-dag variable modules to sys.path and import
# for m in sub_dag_variable_files:
#     sub_dag_module = Path(m).stem
#     # Import and append the variable module
#     module = importlib.import_module(sub_dag_module)
#     imported_sub_dag_modules.append(module)
#
# # Append all sub-dag variable modules to sys.path and import
# for m in sub_dag_table_files:
#     sub_dag_module = Path(m).stem
#
#     # Import and append the variable module
#     module = importlib.import_module(sub_dag_module)
#     imported_sub_dag_modules.append(module)
#
# # Import the required functions for the variables sub-dags
# variable_sub_dag_functions = {imported_sub_dag_modules[i].__name__:
#                                 getattr(imported_sub_dag_modules[i], 'create_variable_subdag')
#                                 for i,_ in enumerate(imported_sub_dag_modules)
#                               if 'table' not in str(imported_sub_dag_modules[i].__name__)}
#
# # Import the required functions for the table sub-dags
# table_sub_dag_functions = {imported_sub_dag_modules[i].__name__:
#                            getattr(imported_sub_dag_modules[i], 'create_table_subdag')
#                            for i,_ in enumerate(imported_sub_dag_modules)
#                            if 'table' in str(imported_sub_dag_modules[i].__name__)}
# TODO: END/
####################################################################################################

# Imported modules list
imported_sub_dag_modules: List[Any] = []

# Imported modules dictionary
imported_sub_dag_modules_dict: Dict[str, Dict[str, Any]] = {}

# Correct algorithm for storing each sub sub-dag file
for root, files, dir in os.walk(os.path.join(pathlib.Path(__file__).parent, 'subdags')):
    if os.path.isdir(root) and '__' not in str(root):
        # Get the sub-dag directory
        current = os.path.split(root)[-1]
        if current != 'subdags':
            imported_sub_dag_modules_dict[current] = {}
            # Append to sys.path
            sys.path.append(root)
            # Grab the modules, excluding `__init__.py` and `__pycache__`
            dir_list = [d for d in dir if '__' not in str(d)]

            # Import or sub-dag modules
            for m in dir_list:
                sub_dag_module = pathlib.Path(m).stem
                module = importlib.import_module(sub_dag_module)
                imported_sub_dag_modules.append(module)

# Load the imported_sub_dag_modules_dict dictionary
for m in imported_sub_dag_modules:
    if 'table' not in str(m.__name__) and 'other' not in str(m.__name__):
        imported_sub_dag_modules_dict['variables'][m.__name__] = getattr(m, 'create_variable_subdag')
    if 'table' in str(m.__name__):
        imported_sub_dag_modules_dict['tables'][m.__name__] = getattr(m, 'create_table_subdag')


# Setup the parent DAG id
DAG_ID = 'master_dag'

# Setup default_args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 22),
    "email": ["will@keplerfi.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2020, 12, 12),
}

# # Create the master dag instance
dag = DAG(DAG_ID, default_args=default_args,
          schedule_interval='@daily')

# Create a list of variable names
variable_names_list = [variable_class_builder.variable_abstractions.get(t_id).__dict__['name']
                       for t_id in list(variable_class_builder.variable_abstractions.keys())]

# Create a list of table names
table_names_list = [table_class_builder.table_abstractions.get(t_id).table_namespace.get('name')
                    for t_id in list(table_class_builder.table_abstractions.keys())]

# Create a list of sub-dag task ids
variable_sub_dag_task_ids = ['one_day_diff_task', 'two_day_diff_task']

table_sub_dag_task_ids = ['one_and_two_day_diff_table_task']


# List of tasks in order of dependency
table_tasks = deque()

# Iterate through all tasks
for k in list(imported_sub_dag_modules_dict.keys()):
    if k == 'variables':
        for i, _ in enumerate(imported_sub_dag_modules_dict['variables']):
            task = SubDagOperator(task_id=variable_sub_dag_task_ids[i],
                                  subdag=imported_sub_dag_modules_dict['variables'] \
                                        .get(list(imported_sub_dag_modules_dict['variables'].keys())[i])(
                                            dag_id='{}.{}'.format(DAG_ID, variable_sub_dag_task_ids[i]),
                                            var_name=variable_names_list[i]),
                                  dag=dag)
            table_tasks.appendleft(task)
    if k == 'tables':
        for i, _ in enumerate(imported_sub_dag_modules_dict['tables']):
            task = SubDagOperator(task_id=table_sub_dag_task_ids[i],
                                  subdag=imported_sub_dag_modules_dict['tables'] \
                                  .get(list(imported_sub_dag_modules_dict['tables'].keys())[i])(
                                      dag_id='{}.{}'.format(DAG_ID, table_sub_dag_task_ids[i]),
                                      name=table_names_list[i]),
                                  dag=dag)
            table_tasks.append(task)

# Set dependency
table_tasks[-2] >> table_tasks[-1]





# TODO: Remove commented out code
####################################################################################################
# # Variable task list
# variable_tasks: List[Any] = []
#
# # Table task list
# table_tasks: List[Any] = []
#
# # Iterate through the variable tasks
# for i, _ in enumerate(variable_sub_dag_task_ids):
#     task = SubDagOperator(task_id=variable_sub_dag_task_ids[i],
#                           subdag=variable_sub_dag_functions.get(list(variable_sub_dag_functions.keys())[i])(
#                               dag_id='{}.{}'.format(DAG_ID, variable_sub_dag_task_ids[i]),
#                               var_name=variable_names_list[i]),
#                           dag=dag)
#
#     variable_tasks.append(task)

# Iterate through the table tasks
# for i, _ in enumerate(table_sub_dag_task_ids):
#     task = SubDagOperator(task_id=table_sub_dag_task_ids[i],
#                           subdag=table_sub_dag_functions.get(list(table_sub_dag_functions.keys())[i])(
#                               dag_id='{}.{}'.format(DAG_ID, table_sub_dag_task_ids[i]),
#                               name=table_names_list[i]),
#                           dag=dag)
#
#     table_tasks.append(task)
####################################################################################################
# TODO: END/





