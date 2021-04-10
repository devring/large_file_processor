import csv
import logging
from contextlib import contextmanager
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
import sqlalchemy
from sqlalchemy.pool import NullPool
import mysql.connector
from Config import Config


class SqlExecute:
    def __init__(self, stage_table):
        self.db_config = Config().db_config
        self.insert_query = """INSERT INTO {0} (name,sku,description) VALUES (%s, %s, %s)""".format(stage_table)
    @contextmanager
    def mysql_connect(self, db_config):
        conn_str = "mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(self.db_config['username'],self.db_config['password'],self.db_config['host'],self.db_config['port'],self.db_config['database'])
        engine = sqlalchemy.create_engine(conn_str, poolclass=NullPool)
        yield engine

        logging.info('Closing connection to server \'%s\'', db_config['host'])
        engine.dispose()
        
    
    @contextmanager
    def mysql_cursor(self, engine):
        cur = engine.connect()
        yield cur
        logging.info('Closing cursor')
        cur.close()

    def execute_query(self, query):
        conn_str = "mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(self.db_config['username'],self.db_config['password'],self.db_config['host'],self.db_config['port'],self.db_config['database'])
        engine = sqlalchemy.create_engine(conn_str, poolclass=NullPool)
        cur = engine.connect()
        result = cur.execute(query)
        cur.close()
        engine.dispose()  
        return result