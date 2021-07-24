import datetime
import errno
import logging
import os
import pandas as pd
from sqlalchemy import create_engine

paths = {
    "local": "/Users/maximerichard/dev/nancy",
    "raspberry": "/home/pi/nancy"
}

logging_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }

def create_log_dir(name, env):
    date = datetime.datetime.now()
    log_directory = date.strftime('{}/logs/%Y_%m_%d'.format(
        paths[env]
    ))
    try:
        os.makedirs(log_directory)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    log_file_name = date.strftime('{}_%d_%m_%Y_%H_%M.log'.format(name))
    return log_directory, log_file_name

def flatten_list(a_list):
    return [element[0] for element in a_list]

class SQLiteExecutor(object):
    """Class for executing SQL statements"""
    def __init__(self, env, logger, path_to_db):
        self.env = env
        self.logger = logger
        self.path_to_db = path_to_db

    @property
    def engine(self, echo=False):
        path_to_db = "{env}/{path_to_db}".format(
            env=paths[self.env],path_to_db=self.path_to_db)
        self.logger.info("[SQLiteExecutor] Path to db: {}".format(path_to_db))
        sqlite_loc = 'sqlite:///{path}'.format(path=path_to_db)
        engine = create_engine(sqlite_loc, echo=echo)
        return engine

    def save_pandas_df(self, df, table, if_exists='append'):
        self.logger.info("[SQLiteExecutor] Saving pandasdf to table: {}".format(
            table))
        df.to_sql(table, self.engine, if_exists=if_exists, index=False)

    def execute_query(self, query):
        with self.engine.begin() as con:
            return con.execute(query).fetchall()

    def insert_query(self, query):
        with self.engine.begin() as con:
            return con.execute(query)