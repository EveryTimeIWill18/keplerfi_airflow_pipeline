"""
table_functions.py
~~~~~~~~~~~~~~~~~~
"""
import sys
sys.path.append('/usr/local/airflow/dags')
# sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
from class_variable_abstractions.table_abstraction_builder import TableClassBuilder
from class_variable_abstractions.variable_creation import variable_creation






def build_joined_table(table_instance: TableClassBuilder, table_name: str, engine_name: str, db_name: str, commit_changes=False):
    # BigQuery testing engine: 'ml-for-fundamental_will_airflow_db'

    variable_creation.create_table_from_abstraction(table_instance=table_instance, table_name=table_name,
                                                    db_name=db_name,
                                                    engine_name=engine_name,
                                                    commit_changes=commit_changes)

