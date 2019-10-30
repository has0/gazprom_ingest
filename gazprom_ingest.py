import os
from pathlib import Path
from datetime import datetime
import fnmatch
import re
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return conn

sql_create_files_ingested_table = '''CREATE TABLE IF NOT EXISTS files_ingested (
    file_name text PRIMARY KEY,
    company_id text NOT NULL,
    ingestion_date text NOT NULL,
    ingestion_time text NOT NULL,
    creation_date text NOT NULL,
    creation_time text NOT NULL,
    file_gen_no text NOT NULL
);'''

sql_create_meter_consumption_table = '''CREATE TABLE IF NOT EXISTS meter_consumption (
    meter_no int,
    measurement_date text,
    measurement_time text,
    consumption double,
    file_name text NOT NULL,
    PRIMARY KEY(meter_no,measurement_date,measurement_time),
    FOREIGN KEY (file_name) REFERENCES files_ingested(file_name)
);'''

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def insert_meter_reading(conn, meter_record):
    
    meter_no,measurement_date,measurement_time,consumption,file_name= meter_record
    sql_meter_record = f'''REPLACE INTO meter_consumption (meter_no, measurement_date, measurement_time
                                         ,consumption,file_name) 
                        VALUES ('{meter_no}', '{measurement_date}', '{measurement_time}'
                                ,'{consumption}','{file_name}');'''  
    cur = conn.cursor()
    cur.execute(sql_meter_record)
    return cur.lastrowid

def insert_file_ingested(conn, file_ingested):

    file_name,company_id,ingestion_date,ingestion_time,creation_date,creation_time,file_gen_no = file_ingested
    sql_file_record = f'''INSERT INTO files_ingested (file_name,company_id,ingestion_date,ingestion_time,creation_date,creation_time,file_gen_no) 
                        VALUES ('{file_name}', '{company_id}', '{ingestion_date}'
                                ,'{ingestion_time}','{creation_date}','{creation_time}'
                                ,'{file_gen_no}');'''  

    cur = conn.cursor()
    print(sql_file_record)
    cur.execute(sql_file_record)
    print('file ingest record inserted')
    return cur.lastrowid

def check_file_ingested(conn, file_name):

    sql_check_exists = f"""SELECT EXISTS(SELECT 1 FROM files_ingested WHERE file_name='{file_name}');"""  

    cur = conn.cursor()
    cur.execute(sql_check_exists)
    rows = cur.fetchall()
 
    return rows


def convert_date(date, time, str_format):
    timestamp = str(int(date)) + " " + str(int(time))
    formated_date = datetime.strptime(timestamp,str_format)
    return formated_date

def check_file_gen(file_gen_id):
    return True if(re.match(r'(PN|DV)[0-9]{6}',file_gen_id)) else False


def check_header(header_record):
    pass_status = False
    if(header_record['record_id'] != 'HEADR'):
        return pass_status
    if(header_record['file_type_id'] != 'SMRT'):
        return pass_status
    if(header_record['company_id'] != 'GAZ'):
        return pass_status
    if(not check_file_gen(header_record['file_gen_no'])):
        return pass_status
    if(convert_date(header_record['file_creation_date'],header_record['file_creation_time'], '%Y%m%d %H%M%S') 
       <= datetime.now()):
        pass_status = True
        return pass_status

def check_footer(footer_record):
    # Expected format for TRAIL - last row:
        #Record Identifier
    return True if footer_record['record_id'] == 'TRAIL' else False


def main():
	conn = sqlite3.connect('gazprom.db')
	cursor = conn.cursor()

	conn = create_connection('gazprom.db')
	# create tables
	if conn is not None:
	    # create projects table
	    create_table(conn, sql_create_files_ingested_table)

	    # create tasks table
	    create_table(conn, sql_create_meter_consumption_table)
	else:
	    print("Error! cannot create the database connection.")

	# List all files in directory using pathlib
	basepath = Path('sample_data/')
	files_in_basepath = (entry for entry in basepath.iterdir() if entry.is_file())
	for file in files_in_basepath:
	    file_name = file.name#[:-5]
	    # Check file is SMRT before 
	    if(fnmatch.fnmatch(file_name,"*.SMRT")):
	        # Extract file metadata
	        info = file.stat()
	        size = info.st_size
	        ctime = datetime.fromtimestamp(info.st_ctime)#.strftime('%Y-%m-%d %H:%M:%S')
	        
	        # check if filename has been loaded in db previously, if yes pass
	        
	        # check header/footer present & in correct format
	        metadata_cols = ['record_id','file_type_id','company_id','file_creation_date','file_creation_time','file_gen_no']
	        with open(basepath / file.name) as temp_file:
	            count = len(temp_file.readlines())
	            metadata_df=pd.read_csv(basepath / file.name, header=None, names=metadata_cols, skiprows=range(1,count-1))
	        
	            if(check_footer(metadata_df.iloc[1]) and check_header(metadata_df.iloc[0])):
	                # load data into db   
	                file_name = file_name
	                ingestion_date = ctime.strftime("%Y-%m-%d")
	                ingestion_time = ctime.strftime("%H:%M:%S")
	                creation_date = datetime.strptime(str(int(metadata_df.iloc[0]['file_creation_date'])),"%Y%m%d").strftime("%Y-%m-%d")
	                creation_time = datetime.strptime(str(int(metadata_df.iloc[0]['file_creation_time'])),"%H%M%S").strftime("%H:%M:%S")
	                company_id = metadata_df.iloc[0]['company_id']
	                file_gen_no = metadata_df.iloc[0]['file_gen_no']
	                ingestion_file = (file_name,company_id,ingestion_date,ingestion_time,creation_date,creation_time,file_gen_no)
	                # read csv
	                col_names = ['record_id','meter_no','measurement_date','measurement_time','consumption']
	                temp_df = pd.read_csv(basepath / file.name, header=None, names=col_names
	                                      , skiprows=1, skipfooter=1, engine='python')
	                # check if already been read in
	                if(check_file_ingested(conn,file_name)[0][0] == 0):
	                    
	                    #read in file
	                    insert_file_ingested(conn, ingestion_file)

	                    for index, meter_reading in temp_df.iterrows():
	                        if(meter_reading['record_id'] == 'CONSU'):
	                            meter_no = meter_reading['meter_no']
	                            measurement_date = meter_reading['measurement_date']
	                            measurement_time = meter_reading['measurement_time']
	                            consumption = meter_reading['consumption']
	                            meter_reading = (meter_no, measurement_date, measurement_time, consumption, file_name)
	                            print('trying insert',meter_reading)
	                            insert_meter_reading(conn,meter_reading)
	                else:
	                    print(f"file, {file_name}, already processed.")
	conn.commit()
	conn.close()
 
if __name__ == '__main__':
    main()




