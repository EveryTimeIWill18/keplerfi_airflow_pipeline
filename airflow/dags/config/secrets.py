"""
secrets.py
~~~~~~~~~~

fixed import issue
"""
import os
from configparser import ConfigParser
import sys
sys.path.append('/usr/local/airflow/dags')
#sys.path.append('/Users/William/PycharmProjects/keplerfi_airflow_pipeline/dags/')
from config.file_paths import CONFIG_INI, DATABASE_CSV


class Secrets(object):
    """This class is used to access secret credentials"""

    @staticmethod
    def _get_json_key():
        """Private Method
        Securely grabs the Json key.

        :returns: json_key
        """
        try:
            config = ConfigParser()
            config.read(CONFIG_INI)
            sections = config.sections()
            json_key = config.get(sections[0], 'JSON')
            #logger.info(info='Json key successfully grabbed')
            return json_key
        except:
            print(f'Could not read json file')
            #logger.error(error=f'Could not read json file')

    def get_json_key(self):
        """Public Method
        Accessor to the private _get_json_key method.

        :returns: json_key
        """
        return self._get_json_key()

    def _get_postgres_connection(self) -> dict:
        """Private Method
        Securely grabs the Postgres database information.

        :returns:
        """
        try:
            config = ConfigParser()
            config.read(CONFIG_INI)
            sections = config.sections()
            host = config.get(sections[1], 'HOST')
            port = config.get(sections[1], 'PORT')
            user = config.get(sections[1], 'USER')
            password = config.get(sections[1], 'PASSWORD')
            dbname = config.get(sections[1], 'DBNAME')

            # Return a dictionary containing the database configuration
            return {'host': host, 'port': port,
                    'user': user, 'password': password,
                    'dbname': dbname}
        except:
            print(f'Could not read Postgres configuration')
            #logger.error(error=f'Could not read Postgres configuration')

    @staticmethod
    def get_postgres_csv_credentials() -> dict:
        """
        Read in the Postgres credentials from a local .csv file.
        """
        try:
            if os.path.isfile(DATABASE_CSV):
                with open(DATABASE_CSV, 'r') as f:
                    lines = [l.strip('\n') for l in f.readlines()]
                    return {'host': lines[0], 'port': lines[1],
                            'user': lines[2], 'password': lines[3],
                            'dbname': lines[4]}
            else:
                raise OSError(f"OSError: Could not find path to database credentials: {DATABASE_CSV}")
        except OSError as e:
            print(e)

    def get_postgres_connection(self) -> dict:
        """Public Method
        Accessor to the private _get_postgres_connection method.

        :returns: dict
        """
        return self._get_postgres_connection()
        #logger.error(error=e)

# Create an instance of the Secrets
secrets = Secrets()

