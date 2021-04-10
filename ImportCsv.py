from Config import Config
from SqlExecute import SqlExecute
import csv
import logging
import multiprocess as mp
from contextlib import contextmanager
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
import sqlalchemy
from sqlalchemy.pool import NullPool
import mysql.connector

class ImportCsv:
    
    def __init__(self,stage_table):
        self.db_config = Config().db_config
        #self.insert_query = Config().insert_query
        self.query = SqlExecute(stage_table)
        
    logging.basicConfig(level=logging.DEBUG)

    def worker(self, rows):
        with self.query.mysql_connect(self.db_config) as conn:
            with self.query.mysql_cursor(conn) as cursor:
                logging.info('Processing batch')
                cursor.execute(self.query.insert_query, rows)

    def get_chunks(self, batch_size, source_file):
        with open(source_file, 'r') as f:
            csv_reader = csv.reader(f, delimiter=',')
            next(csv_reader, None)
            batch_data = []
            batch_count = 0
            for row in csv_reader:
                batch_data.append([v if v != '' else None for v in row])
                batch_count += 1
                if batch_count % batch_size == 0:
                    yield batch_data
                    batch_data = []
            if batch_data:
                yield batch_data