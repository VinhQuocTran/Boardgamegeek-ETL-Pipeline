import logging
import requests
from azure.functions import InputStream
import azure.functions as func
import time
import json
import pandas as pd
import os
import io
import sys
import ast
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import pyodbc
from sqlalchemy.types import NVARCHAR

# Import custom module
dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'custom_module'))
sys.path.append(dir_path)
from utility_function import get_current_hour
from utility_function import init_adls
from utility_function import init_server_db


############## Main function ##############
def main(inputBlob: InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {inputBlob.name}\n"
                 f"Blob Size: {inputBlob.length} bytes")
    
    # Read data from inputBlob and convert to DataFrame
    blob_content = inputBlob.read()
    csv_file_like = io.StringIO(blob_content.decode('utf-8'))
    df = pd.read_csv(csv_file_like,encoding="utf-8")

    # Convert a string representation of dict to dict 
    df['boardgamemechanic'] = df['boardgamemechanic'].apply(ast.literal_eval)
    df['boardgamefamily'] = df['boardgamefamily'].apply(ast.literal_eval)
    df['boardgamepublisher'] = df['boardgamepublisher'].apply(ast.literal_eval)
    df['boardgamedesigner'] = df['boardgamedesigner'].apply(ast.literal_eval)

    # Create master and bridge tables for data modelling, later we will import those tables into Azure SQL Server
    # Master tables
    master_boardgamemechanic_df=pd.DataFrame(create_master_table(df['boardgamemechanic']).items(), columns=['boardgamemechanic_id', 'boardgamemechanic_name'])
    master_boardgamefamily_df=pd.DataFrame(create_master_table(df['boardgamefamily']).items(), columns=['boardgamefamily_id', 'boardgamefamily_name'])
    master_boardgamepublisher_df=pd.DataFrame(create_master_table(df['boardgamepublisher']).items(), columns=['boardgamepublisher_id', 'boardgamepublisher_name'])
    master_boardgamedesigner_df=pd.DataFrame(create_master_table(df['boardgamedesigner']).items(), columns=['boardgamedesigner_id', 'boardgamedesigner_name'])
    master_boardgame_df=df.drop(columns=['boardgamemechanic',
                                      'boardgamefamily',
                                      'boardgamepublisher',
                                      'boardgamedesigner'])

    # Bridge tables
    bridge_boardgamemechanic_df=create_bridge_table(df,'game_id','boardgamemechanic')
    bridge_boardgamefamily_df=create_bridge_table(df,'game_id','boardgamefamily')
    bridge_boardgamepublisher_df=create_bridge_table(df,'game_id','boardgamepublisher')
    bridge_boardgamedesigner_df=create_bridge_table(df,'game_id','boardgamedesigner')
    

    saved_df=[]
    saved_df.append([master_boardgame_df,"master_boardgame.csv"])
    saved_df.append([master_boardgamemechanic_df,"master_boardgamemechanic.csv"])
    saved_df.append([master_boardgamefamily_df,"master_boardgamefamily.csv"])
    saved_df.append([master_boardgamepublisher_df,"master_boardgamepublisher.csv"])
    saved_df.append([master_boardgamedesigner_df,"masterd_boardgamedesigner.csv"])
    saved_df.append([bridge_boardgamemechanic_df,"bridge_boardgamemechanic.csv"])
    saved_df.append([bridge_boardgamefamily_df,"bridge_boardgamefamily.csv"])
    saved_df.append([bridge_boardgamepublisher_df,"bridge_boardgamepublisher.csv"])
    saved_df.append([bridge_boardgamedesigner_df,"bridge_boardgamedesigner.csv"])


    with open("local.settings.json", "r") as config_file:
        conf = json.load(config_file)
        container_name=conf['data_layer']['gold_layer_container']
    adls=init_adls()

    for df in saved_df:
        csv_content = df[0].to_csv(index=False,encoding='utf-8').encode('utf-8')
        name=df[1]
        adls.upload_file_to_container(container_name,csv_content,name)

    # Create table in sql server
    try:
        db=init_server_db()
    except Exception as e:
        print(f"Error {e} in creating connection to DB, please check DB parameters again")

    # Change dtype to NVARCHAR to avoid "????" character when importing into database
    txt_cols = master_boardgamepublisher_df.select_dtypes(include = ['object']).columns


    # Data ingestion 
    logging.info("Start uploading data to Azure SQL Server DB")
    print("Start uploading data to Azure SQL Server DB")
    start_time = time.time()
    try:
        master_boardgame_df.to_sql(name="boardgame", con=db._engine, if_exists='replace', index=False)
        master_boardgamemechanic_df.to_sql(name="boardgamemechanic", con=db._engine, if_exists='replace', index=False)
        master_boardgamefamily_df.to_sql(name="boardgamefamily", con=db._engine, if_exists='replace', index=False)
        master_boardgamepublisher_df.to_sql(name="boardgamepublisher", con=db._engine, if_exists='replace', index=False, dtype = {col_name: NVARCHAR for col_name in txt_cols})
        master_boardgamedesigner_df.to_sql(name="boardgamedesigner", con=db._engine, if_exists='replace', index=False)
        bridge_boardgamemechanic_df.to_sql(name="bridge_boardgamemechanic", con=db._engine, if_exists='replace', index=False)
        bridge_boardgamefamily_df.to_sql(name="bridge_boardgamefamily", con=db._engine, if_exists='replace', index=False)
        bridge_boardgamepublisher_df.to_sql(name="bridge_boardgamepublisher", con=db._engine, if_exists='replace', index=False)
        bridge_boardgamedesigner_df.to_sql(name="bridge_boardgamedesigner", con=db._engine, if_exists='replace', index=False)
    except Exception as e:
        logging.info(f"Error connecting to the database: {e}, please check config parameters again")
        return None
    execution_time = time.time() - start_time
    print(f"uploading data completed, total second takes to upload to SQL server DB: {int(execution_time)}s")
    logging.info("Uploading data completed")
    


############## Custom function ##############
def create_master_table(list_of_dicts: list) -> dict:
    """
    Create UNIQUE key-value pairs in a list of dicts
    :param list_of_dicts: a list of dicts need need to find unique key-value
    Example input and output: [{'a':1,'b':2},{'b':2,'c':3}] -> {'a':1,'b':2,'c':3}
    """
    return dict(set(pair for d in list_of_dicts for pair in d.items()))

def create_bridge_table(df,baseline_column: str,column_need_to_explode: str) -> pd.DataFrame:
    """
    Create bridge table from Dataframe
    :param df
    :param baseline_column
    :param column_need_to_explode: a name of exploded column, this column must have type of dict
    Example input and output:
    ***INPUT***
    -> df: 
    emp_id,emp_job_title
    1,"{'4':'accountant', '2':'manager'}"
    2,"{'2':'manager', '3':'security'}"
    3,"{'3':'security', '2':'manager'}"

    -> baseline_column: "emp_id"
    -> column_need_to_explode: "emp_job_title"

    ***OUTPUT***
    emp_id,emp_job_title_id
    1,4
    1,2
    2,2
    2,3
    3,3
    3,2
    """

    bridge_df=pd.DataFrame()
    bridge_df[baseline_column]=df[baseline_column]
    bridge_df[column_need_to_explode+"_id"]=df[column_need_to_explode].apply(list)
    bridge_df = bridge_df.explode(column_need_to_explode+"_id")
    return bridge_df