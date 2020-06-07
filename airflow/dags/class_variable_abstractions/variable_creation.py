"""
variable_creation.py
~~~~~~~~~~~~~~~~~~~~
This script will grab the generated variables from the variables Python module and create a table from the most
recent dictionary.
"""
import os
import sys
sys.path.append('/usr/local/airflow/dags')
# sys.path.append('/Users/William/PycharmProjects/dynamic_subdag_setup/dags/')
import json
import pathlib
import sqlalchemy
from pprint import pprint
from itertools import combinations
from config.secrets import secrets
from importlib import import_module
from sqlalchemy.sql import text
from sqlalchemy.engine import create_engine
from typing import List, Dict, Any, Optional, Tuple, Iterable, Sequence, Mapping
from class_variable_abstractions.variable_abstraction_builder import variable_class_builder


class VariableDatabaseCreation(object):
    """Creates the variables from the auto generated variable Python scripts."""

    def __init__(self):
        self.postgres_engines: List[Dict[str, Any]] = []
        self.bigquery_engines: List[Dict[str, Any]] = []
        self.bigquery_queries: Dict[str, Any] = {}
        self.imported_modules: Dict[str, Any] = {}
        self.imported_table_modules: Dict[str, Any] = {}


    def build_big_query_engine(self,  project_name: str, dataset_name: str, echo=True) -> sqlalchemy.engine.base.Engine:
        """Create a BigQuery engine.

        """
        # Create the uri string
        uri_ = 'bigquery://{}/{}'.format(project_name, dataset_name)

        # Create a BigQuery engine
        engine_ = create_engine(uri_, credentials_path=secrets.get_json_key(), echo=echo)

        # Append the new BigQuery engine to the list of dictionaries
        self.bigquery_engines.append({'{}_{}'.format(project_name, dataset_name): engine_})

        # Return the newly created engine
        return engine_


    def get_engine(self, engine_name: str) -> sqlalchemy.engine.base.Engine:
        """Grabs the requisite engine
        :param engine_name[str]: The engine name

        :return engine:
        """
        # Get the correct engine
        engine = [d[engine_name] for d in self.bigquery_engines
                  if list(d.keys())[0] == engine_name][0]

        return engine

    def import_variable_module(self, module_dir: str, module_name: str, dest_dir: str, write_to_file=False):
        """Create the new variable table from the variables/var.py modules"""
        sys.path.append(module_dir)
        variable_module_name = os.path.join(module_dir, module_name + '.py')

        variable_module = pathlib.Path(variable_module_name).stem

        # Import the variable module
        module = import_module(variable_module)

        # Update the imported modules dict
        self.imported_modules[str(variable_module)] = module

        # Grab the query from the current variable
        current_variable_query = module.__dict__['var_'].get('query_logic')


        # Extract the table creation information and format it into a create table statement
        query_dict = {module_name: current_variable_query}
        self.bigquery_queries.update(query_dict)

        if write_to_file:
            # Write the query to price_difference dir
            if os.path.isdir(dest_dir):
                file_name = os.path.join(dest_dir, module_name + '.py')
                with open(file_name, 'w+') as f:
                    # Write the variable to the correct path
                    f.write('var_ = {}'.format(str(query_dict)))

    def create_variable_table(self, engine_name: str, var_name: str, module_name: str, query_string_=None, commit_changes=False):
        """Create the variable table in Big Query"""
        # Get the engine
        engine = self.get_engine(engine_name=engine_name)

        # Get the query string
        query_string = self.imported_modules[module_name].__dict__['var_']['query_logic']
        #query_string = variable_class_builder.get_variable_query(var_name=var_name)

        # Create a database connection
        conn = engine.connect()

        # Create the variable table
        create_variable_query_string = """CREATE TABLE IF NOT EXISTS {} AS {};""".format(var_name, query_string_)

        if commit_changes:
            # Commit the new table
            conn.execute(text(create_variable_query_string))

    def create_variable_table_from_json(self, var_name: str):
        """Create the variable table from a json file in SQL"""
        # Get the path to the variables directory
        variables_dir = os.path.join(
            pathlib.Path(__file__).parent.parent.absolute(), 'variables')

        current_var_file = os.path.join(variables_dir, var_name + '.json')
        if os.path.isfile(current_var_file):
            with open(current_var_file, 'r') as f:
                variable_dict = json.load(f)
                print(f'VARIABLE_DICT: {variable_dict}')




    def create_joined_variable_table(self, engine_name: str, new_table_name: str, columns: List[str],
                                     join_on: List[str], commit_changes=False, **modules):
        """Create the final joined table"""

        # Get the required engine
        engine = self.get_engine(engine_name=engine_name)

        # Create a database connection
        conn = engine.connect()

        # Set up a list of letters that will be used for the sub queries
        letters = [chr(x) for x in range(ord('a'), ord('z') + 1)]

        select_query_string = " ".join(['{},'.format(c) for c in columns]).rstrip(',')
        select_query_string = select_query_string + " from "

        module_query_list = []
        letter_counter = 0
        for k, v in modules.items():
            mod_str = '(' + v + ') ' + letters[letter_counter]
            module_query_list.append(mod_str)
            letter_counter += 1

        # Properly format the join on values
        join_ons = " ".join('{} and'.format(i) for i in join_on).rstrip(' and')

        # Properly format the joined queries
        joined_queries = " ".join(['{} join '.format(q) for q in module_query_list]).rstrip(' join')

        # Fully formatted join query
        full_joined_query = 'select ' + select_query_string + " " + joined_queries + " on " + join_ons

        # Final create table query
        create_table_query = """CREATE TABLE IF NOT EXISTS {} as {};""".format(new_table_name, full_joined_query)

        if commit_changes:
            # Commit the new table
            conn.execute(text(create_table_query))

    def updated_create_joined_variable_table(self, engine_name: str, new_table_name: str,
                                     commit_changes=False, **tables):
        """Create the final joined table"""

        # Get the required engine
        engine = self.get_engine(engine_name=engine_name)

        # Create a database connection
        conn = engine.connect()

        # Set up a list of letters that will be used for the sub queries
        letters = [chr(x) for x in range(ord('a'), ord('z') + 1)]

        # Create the query strings
        select_query_string_new = "".join(['{}.value as var_{}, '.format(letters[i], i+1)
                                           for i, _ in enumerate(list(tables.keys()), start=0)]).rstrip(' ,')

        print(f'SELECT QUERY NEW: {select_query_string_new}')

        # select_difference_query = "".join(['{}.value/{}.value - 1 as value_{}, '.format(letters[i], letters[i+1], i+1)
        #                                    for i in range(len(tables.keys())-1)]).rstrip(' ,')

        # Concatenate query strings into a single string
        select_query_string = "select a.date, a.ticker, "  + select_query_string_new + " from "

        print(f'SELECT QUERY STRING: {select_query_string}')

        module_query_list = []
        letter_counter = 0


        for v in tables.values():
            mod_str = '( select date, ticker, value from ' + v + ') ' + letters[letter_counter]
            module_query_list.append(mod_str)
            letter_counter += 1

        # Properly format the join on values
        # join_ons = " ".join('{} and'.format(i) for i in join_on).rstrip(' and')
        join_ons = " on a.ticker=b.ticker and a.date=b.date"

        # Properly format the joined queries
        joined_queries = " ".join(['{} left outer join '.format(q) for q in module_query_list]).rstrip(' left outer join')

        # Fully formatted join query
        full_joined_query = select_query_string + " " + joined_queries + join_ons

        print(f'FULL JOINED QUERY: {full_joined_query}')
        # Final create table query
        create_table_query = """CREATE TABLE IF NOT EXISTS {} as {};""".format(new_table_name, full_joined_query)
        print(f'CREATE TABLE QUERY: {create_table_query}')
        if commit_changes:
            # Commit the new table
            conn.execute(text(create_table_query))


    def create_table_from_abstraction(self, table_instance, table_name: str, engine_name: str,
                                      db_name: str, commit_changes=False):
        """Create the table in the database from the table abstraction file
        """

        # Grab the table instance
        current_table = table_instance.table_abstractions.get(table_name)

        # Get the required engine
        engine = self.get_engine(engine_name=engine_name)

        # Create a database connection
        conn = engine.connect()

        # To create the Joins, we need n-1 total join logic
        join_logic = current_table.table_namespace.get('variable_components')

        # Set up a list of letters that will be used for the sub queries
        letters = [chr(x) for x in range(ord('a'), ord('z') + 1)]

        # Create the query strings
        select_query_string = "".join(['{}.value as {}, '.format(letters[i], list(join_logic.keys())[i])
                                           for i, _ in enumerate(list(join_logic.keys()), start=0)]).rstrip(' ,')

        # The SELECT portion of the CREATE TABLE statement
        full_select_query_string = " SELECT a.date, a.ticker, " + select_query_string

        # Grab the first joined table as the logic is different for this table
        first_join_value = db_name + "." + str(list(join_logic.keys())[0]) + " a"


        # New join logic
        new_join_logic = "".join([' {} {}.{} {} ON a.ticker = {}.ticker AND a.date = {}.date '.format(join_logic.get(list(join_logic.keys())[i]),
                                                        db_name, list(join_logic.keys())[i+1], letters[i+1], letters[i+1], letters[i+1])
                                  for i in range(len(join_logic.keys()) -1)])

        # Final FROM portion fo the query string
        from_query_string = " FROM " + first_join_value + new_join_logic

        # Create the final SELECT query string
        final_select_query_string = full_select_query_string + from_query_string

        # Create the CREATE TABLE query string
        create_table_string = "CREATE TABLE IF NOT EXISTS {}.{}".format(db_name, table_name.lower()) \
                             + " AS " + final_select_query_string

        print(create_table_string)

        if commit_changes:
            conn.execute(create_table_string)


    def create_big_query_table(self, engine_name: str, table_name: str, **columns):
        """Create the Big Query table"""

        # Get the required engine
        engine = self.get_engine(engine_name=engine_name)

        # Create a database connection
        conn = engine.connect()

        # Format the column information into a string
        columns_str = " ".join(["{} {},".format(k, v) for k, v in columns.items()]).rstrip(",")

        # Build the query string
        create_table_stmt = """CREATE TABLE IF NOT EXISTS {} ({});""".format(table_name, columns_str)

        # Commit the new table
        conn.execute(text(create_table_stmt))

    def insert_into_table(self, engine_name: str, table_name: str,  source_query: str, commit_changes=False):
        """Insert data into a given BigQuery table"""
        # Get the required engine
        engine = self.get_engine(engine_name=engine_name)

        # Create a database connection
        conn = engine.connect()

        insert_stmt = """INSERT INTO {} {}""".format(table_name, source_query)


        if commit_changes:
            # Commit the new table
            conn.execute(text(insert_stmt))

        return insert_stmt

    def create_from_partition(self, engine_name: str, source_query: str, commit_changes=False):
        """Create and insert data into a partitioned table"""
        # Get the required engine
        engine = self.get_engine(engine_name=engine_name)

        # Create a database connection
        conn = engine.connect()

        if commit_changes:
            # Commit partitioned table
            conn.execute(text(source_query))




# Testing #
#################################################
variable_creation = VariableDatabaseCreation()

bq_engine = variable_creation.build_big_query_engine(project_name='ml-for-fundamental', dataset_name='will_airflow_db')



