from SqlExecute import SqlExecute
from ImportCsv import ImportCsv
from Config import Config
import csv
import logging
import multiprocess as mp
from contextlib import contextmanager
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
import sqlalchemy
from sqlalchemy.pool import NullPool
import mysql.connector

logging.basicConfig(level=logging.DEBUG)

def insert_to_prod(stage_table,prod_table,sql_execute):
    file_location = raw_input('Please input the location of the CSV file to ingest\n')
    insert_data = ImportCsv(stage_table)
    batch_size = 5000
    chunk_gen = insert_data.get_chunks(batch_size, file_location)
    pool = mp.Pool(mp.cpu_count()-1)
    results = pool.map(insert_data.worker, chunk_gen)
    pool.close()
    pool.join()
    logging.info('Insert to Stage Complete complete. Insert to Prod Table Start')
    result = sql_execute.execute_query("SELECT COUNT(1) FROM {0}".format(prod_table))
    count = result.fetchone()[0]
    if count == 0:
        insert_query = """INSERT INTO {1} SELECT name,sku,description FROM {0}""".format(stage_table, prod_table)
        logging.info('Bulk Insert from stage to prod table (One time Full Load)')
        sql_execute.execute_query(insert_query)
    else:
        logging.info('Inserting New Records to Prod')
        new_records = """INSERT INTO {1} SELECT ps.name, ps.sku, ps.description FROM {0} ps left join {1} pp on ps.name = pp.name and ps.sku = pp.sku and ps.description = pp.description WHERE pp.sku is NULL""".format(stage_table, prod_table)
        sql_execute.execute_query(new_records)

    logging.info('Prod Table insertion Complete. Truncating Stage Table')
    sql_execute.execute_query("TRUNCATE TABLE {0}".format(stage_table))
    logging.info('File Ingestion Succeeded. Please query {0} Table to check the results.'.format(prod_table))


def update_sku(prod_table,sql_execute):    
    sku = raw_input("Enter the sku that you would like to update\n")
    check_record = """SELECT COUNT(1) FROM {1} where sku = '{0}'""".format(sku, prod_table)
    result = sql_execute.execute_query(check_record)
    count = int(result.fetchone()[0])
    if count == 0:
        print("The given sku does not exist. Exiting Program")
        return False
    else:
        name = raw_input("Enter the name of the sku that you would like to update\n")
        check_record = """SELECT COUNT(1) FROM {2} where sku = '{0}' and name = '{1}'""".format(sku, name, prod_table)
        result = sql_execute.execute_query(check_record)
        count = int(result.fetchone()[0])
        if count == 0:
            print("The given name does not exist. Exiting Program")
            return False
        description = raw_input("Enter the new description of the sku\n")
        update_query = """UPDATE {3} SET description = '{0}' WHERE sku = '{1}' and name = '{2}'""".format(description, sku, name, prod_table)
        sql_execute.execute_query(update_query)
        print("The sku has been updated successfully.")



######
#Script Execution Start
######

prod_table = 'products_prod3'
stage_table = 'products_stage3'
sql_execute = SqlExecute(stage_table)
while True:
    print("""Please see the below options to choose how to proceed further:
    Press 1: For Inserting Data into Table from CSV File
    Press 2: To Update the existing sku
    Press 3: To Exit the Program.""")
    option = int(input('Please input the right option to proceed further\n'))
    if option == 1:
        insert_to_prod(stage_table,prod_table,sql_execute)
        print("CSV File Ingested to {0} Table Successfully".format(prod_table))
    elif option == 2:
        update_sku(prod_table,sql_execute)
    elif option == 3:
        print("Exiting Program...")
        break
    else:
        print("Option entered is not Valid.Please enter option again.")
