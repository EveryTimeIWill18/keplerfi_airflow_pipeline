"""
table_abstraction_builder.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fixed import issue
"""
import os
import sys
sys.path.append('/usr/local/airflow/dags')
# sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
import pathlib
from typing import List, Dict, Optional, Any, AnyStr
from class_variable_abstractions.table_abstractions import TableAbstraction
from pprint import pprint

class TableClassBuilder:
    """This class uses both the Builder and Singleton creational
        design patterns. The goal of this class is to create new table
        abstractions and store the results in a singleton
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            print(f'Creating object: {cls.__class__.__name__}\nID: {id(cls)}')
            cls._instance = super(TableClassBuilder, cls).__new__(cls)
        else:
            raise Exception(f'ERROR: Can only create once TableClassBuilder instance')
        return cls._instance

    def __init__(self):
        self.table_abstractions: Dict[str, Any] = {}

    def create_new_table(self, name: str, description: str, dialect: str,
                         variable_components: Dict[str, Any], schedule: str, depends: bool,
                         provide_context: bool, op_kwargs: Dict[str, Any], dag_id: str) -> None:
        """Add a new table abstraction"""
        # Create an instance of TableAbstraction
        table_abstraction = TableAbstraction()

        # Set the name
        table_abstraction.table_name = name

        # Set the description
        table_abstraction.table_description = description

        # Set the dialect
        table_abstraction.table_dialect = dialect

        # Set the variable components
        table_abstraction.table_variable_components = variable_components

        # Set the schedule
        table_abstraction.table_run_schedule = schedule

        # Set the past dependency
        table_abstraction.table_depends_on_past = depends

        # Set provide_context
        table_abstraction.set_provide_context(provide_context)

        # Set op_kwargs
        table_abstraction.set_op_kwargs(op_kwargs)

        # Set dag_id
        table_abstraction.set_dag_id(dag_id)


        # Add the new table abstraction to self.table_abstractions
        self.table_abstractions[name] = table_abstraction





    def generate_table_python_file(self, table_name: str):
        """Creates a new table.py file that contains the information
                       required to create a new joined table.

                       :param var_name[str]:
        """
        # Get the path to the variables directory
        tables_dir = os.path.join(
            pathlib.Path(__file__).parent.parent.absolute(),
            'tables', 'price_difference'
        )

        if os.path.isdir(tables_dir):
            table_ = self.table_abstractions.get(table_name)
            # Create the python file
            file_name = os.path.join(tables_dir, table_name + '.py')
            # Create the file
            with open(file_name, 'w+') as f:
                f.write(f'"""{table_.__dict__["_name"]}"""\n# Updated at '
                        f'\nvar_ = {table_.__dict__}\n')


# Testing #
####################################################################################################
table_class_builder = TableClassBuilder()

