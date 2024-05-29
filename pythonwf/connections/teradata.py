import teradataml
from teradataml import DataFrame, get_connection, execute_sql
import pandas as pd
from packaging import version
import atexit
import sys

from pythonwf.clean_up.clean_up import TrackSQL


class TeradataHandler:
    def __init__(self, host, user, password, logmech='KRB5'):
        self.host = host
        self.user = user
        self.password = password
        self.context = None
        self.connection = None
        self.logmech = logmech
        self.tracking = TrackSQL(self)
        self.teradataml_version = teradataml.__version__

    def connect(self):
        self.context = teradataml.create_context(
            host=self.host,
            username=self.user,
            password=self.password,
            logmech=self.logmech
        )
        self.connection = get_connection()

        # Register cleanup function to be called on program exit
        atexit.register(self.cleanup)

    def disconnect(self):
        if self.context:
            teradataml.remove_context()
            self.context = None

    def execute_query(self, query):
        # Check the teradataml version
        if version.parse(self.teradataml_version) > version.parse("17.20.0.03"):
            return execute_sql(query)
        else:
            return self.connection.execute(query)

    def to_pandas(self, query):
        tf = DataFrame.from_query(query)
        return tf.to_pandas()

    def fastexport(self, query):
        tf = DataFrame.from_query(query)
        return teradataml.fastexport(tf)

    def cleanup(self):
        try:
            self.tracking.clean_up()
        except Exception as e:
            print(f"An error occurred during cleanup: {e}", file=sys.stderr)
        finally:
            self.disconnect()

    def __del__(self):
        self.cleanup()

