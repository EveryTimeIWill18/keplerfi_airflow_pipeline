"""
file_paths.py
~~~~~~~~~~~~~
TODO: Finish module level doc string

Good, no import issues
"""
import os
import sys
from pathlib import Path
from functools import wraps

print(sys.version)



# File Paths #
####################################################################################################
# Create the generate_file_path decorator
# def generate_file_path(func: callable) -> callable:
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         f = func(*args, **kwargs)
#         PATH = os.path.join(
#             Path(__file__).parent.parent.parent.parent.parent,
#             'hidden_data', f
#         )
#         return PATH
#     return wrapper

# # add the generate_file_path decorator to the function
# @generate_file_path
# def create_path(file_path: str) -> str:
#     return file_path


# Create the static file paths in Docker container
#CONFIG_INI = '/usr/local/hidden/config.ini'
#DATABASE_CSV = '/usr/local/hidden/database_config.csv'


# Path to config file
CONFIG_INI = os.path.join(
    Path(__file__).parent.parent.parent.parent,
    'hidden_data', 'config.ini'
    )

DATABASE_CSV = os.path.join(
    Path(__file__).parent.parent.parent.parent,
    'hidden_data', 'database_config.csv')



# To be used in Keplerfi gcloud compute
# CONFIG_INI = '/home/will/etl_airflow_pipeline/hidden/config.ini'
#DATABASE_CSV = '/home/will/etl_airflow_pipeline/hidden/database_config.csv'

# Create the static file paths
# CONFIG_INI = '/Users/William/hidden_data/config.ini'
# DATABASE_CSV = '/Users/William/hidden_data/database_config.csv'