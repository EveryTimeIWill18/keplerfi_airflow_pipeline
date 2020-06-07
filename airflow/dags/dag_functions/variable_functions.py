"""
variable_functions.py
~~~~~~~~~~~~~~~~~~~~~
fixed import issue
"""
import sys
sys.path.append('/usr/local/airflow/dags')
# sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
from class_variable_abstractions.variable_abstraction_builder import variable_class_builder
from class_variable_abstractions.variable_creation import variable_creation



def create_var_table(dataset_name: str, project_name: str, table_name: str, **kwargs):
	"""

	:param dataset_name:
	:param project_name:
	:param table_name:
	:param kwargs:
	:return:
	"""
	# Create the engine
	variable_creation.build_big_query_engine(project_name=project_name, dataset_name=dataset_name)

	# Create the variable table
	variable_creation.create_big_query_table(engine_name=project_name + "_" + dataset_name, table_name=table_name, **kwargs)


def insert_data(ds, project_name: str, dataset_name: str, module_dir: str, module_name: str, table_name: str,
				dest_dir: str, commit_changes=False, **kwargs):
	"""

	:param ds:
	:param project_name:
	:param dataset_name:
	:param module_dir:
	:param module_name:
	:param table_name:
	:param dest_dir:
	:param commit_changes:
	:param kwargs:
	:return:
	"""
	# Create the engine
	variable_creation.build_big_query_engine(project_name=project_name, dataset_name=dataset_name)

	# Import the variable module
	# variable_creation.import_variable_module(module_dir=module_dir, module_name=module_name, dest_dir=dest_dir)

	# Grab the sql query and replace <DATEID> with backfill date
	sql_query = variable_class_builder.variable_abstractions.get(module_name) \
				.__dict__["query_logic"] \
				.replace("\n","") \
				.replace("\t", "") \
				.replace("<DATEID>", ds)

	print(f'QUERY_LOGIC: {sql_query}')

	# Insert data into the table
	variable_creation.insert_into_table(engine_name=project_name + "_" + dataset_name, table_name=table_name,
										source_query=sql_query, commit_changes=commit_changes)


def build_partitioned_table(ds, dataset_name: str, project_name: str, module_name: str, commit_changes=False):
	"""
	NOTE: USE ONLY THIS METHOD IF CREATING A PARTITIONED TABLE
	:param ds:
	:param dataset_name:
	:param project_name:
	:param module_name:
	:param commit_changes:
	:return:
	"""
	# Create the engine
	variable_creation.build_big_query_engine(project_name=project_name, dataset_name=dataset_name)

	sql_query = variable_class_builder.variable_abstractions.get(module_name) \
				.__dict__['query_logic'] \
				.replace("<DATEID>", ds)

	# Create and insert the data into the partitioned table
	variable_creation.create_from_partition(engine_name=project_name + "_" + dataset_name,
											source_query=sql_query, commit_changes=commit_changes)
