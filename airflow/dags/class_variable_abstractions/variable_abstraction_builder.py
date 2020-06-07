"""
variable_abstraction_builder.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This module uses the builder design pattern to create and store new instances of the variable abstractions.


TODO: Finish all other methods that are part of the DataComponents class

Good, fixed import issues
"""
import os
import sys
sys.path.append('/usr/local/airflow/dags')
# sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
import json
import shutil
import subprocess
import pathlib
from datetime import datetime
from pprint import pprint
from typing import List, Dict, Optional, Any, AnyStr
from class_variable_abstractions.variable_abstractions import DataClassComponents


class VariableClassBuilder:
    """This class uses both the Builder and Singleton creational
    design patterns. The goal of this class is to create new variable
    abstractions and store the results in a singleton
    """
    _instance = None

    # TO BE REMOVED. FOR TESTING PURPOSES ONLY
    MODULE_DIR = '/Users/William/PycharmProjects/dynamic_subdag_setup/dags/variables'
    DEST_DIR = '/Users/William/PycharmProjects/dynamic_subdag_setup/dags/tables'

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            print(f'Creating object: {cls.__class__.__name__}\nID: {id(cls)}')
            cls._instance = super(VariableClassBuilder, cls).__new__(cls)
        else:
            raise Exception(f'ERROR: Can only create once VariableClassBuilder instance')
        return cls._instance

    def __init__(self):
        self.variable_abstractions: Dict[str, Any] = {}

    def create_new_variable(self, var_name: str, data_source: str, data_destination: str,
                    description: str, query_logic: str, data_provenance: str, data_quality: str,
                    variable_type: str, schedule: str, start_date: datetime,
                            provide_context: Dict[str, bool], op_kwargs: Dict[str, Dict[str, Any]], dag_id: str,
                            from_file=False, query_=None):
        """Add a data class component to the variable_abstractions data structure"""
        # Create an instance of DataClassComponents
        data_cls_component = DataClassComponents()

        # Set the variable's name
        data_cls_component.set_name(name=var_name)

        # Set the variable's data source
        data_cls_component.set_data_source(data_source)

        # Set the variable's destination
        data_cls_component.set_data_destination(data_destination)

        # Set the variable's description
        data_cls_component.set_description(description)

        # Set the variable's data provenance
        data_cls_component.set_data_provenance(data_provenance)

        # Set the variable's data quality
        data_cls_component.set_data_quality(data_quality)

        # Set the variable's data type
        data_cls_component.set_variable_type(variable_type)

        # Set the variable's query logic
        data_cls_component.set_query_logic(query_logic, from_file, query_)

        # Set the variable's schedule
        data_cls_component.set_schedule(schedule)

        # Set the variable's start_date
        data_cls_component.set_start_date(start_date)

        # Set the variable's provide_context
        data_cls_component.set_provide_context(provide_context)

        # Set the variable's op_kwargs
        data_cls_component.set_op_kwargs(op_kwargs)

        # Set the variable's dag_id
        data_cls_component.set_dag_id(dag_id)

        # Update the variable_abstractions data structure
        self.variable_abstractions.update({var_name: data_cls_component})

        return self.variable_abstractions.get(var_name)

    def get_variable_info(self, var_name: str, var_key: str):
        """Get a specific element from the variable class instance"""
        var_ = self.variable_abstractions.get(var_name)
        return var_.__dict__[var_key]

    def update_variable_info(self, var_name: str, var_key: str, update_value: str):
        """Updates a specific parameter within the variable class instance"""
        var_ = self.variable_abstractions.get(var_name)
        var_.__dict__[var_key] = update_value

    def generate_variable_subdag(self, var_name: str):
        """Create the subdag script for the current variable"""
        # Get the path to the variables directory
        subdag_dir = os.path.join(
            pathlib.Path(__file__).parent.parent.absolute(), 'subdags')

        if os.path.isdir(subdag_dir):
            print('subdag dir exists')
            # Create the python file
            file_name = os.path.join(subdag_dir, var_name + '.py')

            with open(file_name, 'w+') as f:
                f.write('"""\n{}\n"""\nfrom airflow import DAG'
                        '\nfrom datetime import datetime, timedelta'
                        '\nfrom class_variable_abstractions.variable_abstraction_builder import variable_class_builder'
                        '\nfrom airflow.operators.python_operator import PythonOperator'
                        '\nfrom class_variable_abstractions.variable_creation import variable_creation, module_dir, dest_dir, table_module'
                        '\nfrom dag_functions.variable_functions import create_var_table, insert_data'
                        '\n\n'
                        '\ndef create_variable_subdag():'
                        '\n\t"""Creates the variable sub-dag"""'
                        '\n\t'
                        '\n\tvariable_namespace = variable_class_builder.variable_abstractions.get("{}")'
                        '\n\t# Create a dag instance'
                        '\n\tdag = DAG(dag_id="{}", schedule_interval=variable_namespace.get("schedule"), start_date=variable_namespace.get("start_date"))'
                        '\n\t'
                        '\n\tcreate_variable_table = PythonOperator(task_id="create_{}", python_callable=create_var_table, provide_context={}, op_kwargs={}, dag=dag)'
                        '\n\tinsert_into_variable_table = PythonOperator(task_id="insert_{}", python_callable=insert_data, provide_context={}, op_kwargs={}, dag=dag)'
                        '\n\t return dag' \
                                                                        .format(var_name,
                                                                                var_name,
                                                                                self.variable_abstractions.get(var_name).__dict__['dag_id'],
                                                                                var_name,
                                                                                self.variable_abstractions.get(var_name).__dict__['provide_context'],
                                                                                self.variable_abstractions.get(var_name).__dict__['op_kwargs'].get('create_var_table'),
                                                                                var_name,
                                                                                self.variable_abstractions.get(var_name).__dict__['provide_context'],
                                                                                self.variable_abstractions.get(var_name).__dict__['op_kwargs'].get('insert_data')))
    def get_variable_query(self, var_name: str) -> str:
        """Create the variable table in BigQuery"""
        # Grab the query string
        query_string = self.get_variable_info(var_name=var_name, var_key='query_logic')

        return query_string


# Create the VariableClassBuilder Instance #
########################################################################################################################
variable_class_builder = VariableClassBuilder()

