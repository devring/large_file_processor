# large_file_processor
## Data Ingestion Pipeline for processing large CSV Files.

#### Database Used : MySql

#### Python Version: 2.x/3.x

#### Operating System: Any Linux Distribution (I have used Ubuntu). The scripts are to be run on the command line.

#### Required Python Modules to be installed before running the scripts:
1. multiprocess
2. sqlalchemy
3. mysql.connector






### Project WorkFlow:

Data_Ingestion.py ----->  MySql Staging Table ------> MySql Prod Table  ( --> Aggregated Table is built after insertion to Prod Table. You can see the number of products for each name in this table.)

The approach used to ingest the data consists of two tables. First we ingest the CSV File to the Staging table. Then a comparison of data is done between the Staging and the Prod Table.
Only records which are present in Staging and not present in Prod are inserted into the Prod Table. Then an aggregation query runs and inserts grouped data to Group Table.

Data Ingestion to staging table is done in batches. You can set the batch size in Data_Ingestion.py. Default value for batch_size = 5000.
A parallel non-blocking ingestion is achieved using multiprocessing. But the default order of the records is lost because of the parallel inserts.






### Table Schemas for MySql

Stage Table:
create table products_stage ( name varchar(255), sku varchar(255),
description varchar(255));

Prod Table:
create table products_prod ( name varchar(255), sku varchar(255),
description varchar(255));

Group Table:
create table product_group (name varchar(255), total int);





### Steps to run the file processing scripts:

1. Enter the Mysql Database credentials and database name in Config.py
2. Create the 3 required tables in MySql Database. Use the above mentioned commands to recreate the tables in the database you mentioned in the previous step.
3. Change the Mysql Table variable names for Stage Table and Prod Table in Data_Ingestion.py (Change the values for the variables --> stage_table and prod_table)
4. Then Run the script Data_Ingestion.py using this command ----> python Data_Ingestion.py
5. Follow the command line instructions and give the location of the file as input.
6. If it is the first time inserting to the table then a full load from Stage Table is done to the Prod Table.
7. If it is not the first time, then a comparison of records is done between Stage and Prod and only new records are inserted. (Comparison is done using name and sku keys)
8. If you want to update a record, follow the command line instructions and update the records by giving valid sku and name as input.
9. The Group Table is created to view the number of products under each name.



#### Commands to run on the command line
python Data_Ingestion.py



### Points that have been achieved
1. Follows the OOPS concept. Seperate Classes are created keeping each functionality in mind and a proper abstraction is created.
2. Regular non-blocking parallel ingestion has been achieved using Multi-Processing. Parallel inserts are done using multiple connections.
3. Updating any product is achieved using the sku and name key. It is not possible to update using only sku key as it is not a primary key.
4. All product details have been ingested into one single table.
5. An aggregated table to find the total number of products for each name with `name` and `total` as the columns has been achieved.

#### Please refer to the screenshots of the Prod Table and Group Table in the Repo.






### Things I would improve if given more days
1. Clean the code - The code looks a little messy, I would take some time and structure it better.
2. Get a better understanding - The above approach came from my little understanding of the problem by reading the assignment form. 
   It would have been better if I was able to communicate with the interview team and ask a few questions to get a better understanding.
3. Change the design - By getting a better understanding, I want to change the design. I can implement the same thing done in this code by using Pandas Dataframe or Spark and simplify the entire process.
   But storing so many records in-memory and doing comparisons is not really scalable. This is not reliable when the data is huge and will crash the system.
   This is the reason I am doing data comparison in the database server. 
   
   The comparison can be improved by generating a hash-key for each record and stored in the database. With each insertion, new hash-keys generated from CSV file
   will be compared with the ones stored in the database. This will save a lot of time in comparing data.
   
4. Search for approaches that may be faster than the one implemented for CSV Ingestion.


