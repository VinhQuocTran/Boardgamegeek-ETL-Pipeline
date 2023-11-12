import requests 
import pandas as pd
import numpy as np
import re
import time
import os
import json
import sys
from xml.etree import ElementTree
from datetime import datetime
from adls_module import ADLSModule
from azure_db_module import AzureSQLDatabaseModule


def init_adls():
    """
    Read setting parameters from conf file and init ADLS
    """
    with open("local.settings.json", "r") as config_file:
        conf = json.load(config_file)
        connection_string = conf["adls"]["connection_string"]
        key = conf["adls"]["key"]
        sa_name = conf["adls"]["sa_name"]
                
    adls=ADLSModule(sa_name,connection_string,key)
    return adls


def init_server_db():
    """
    Read setting parameters from conf file and init Azure SQL Server DB
    """
    with open("local.settings.json", "r") as config_file:
        conf = json.load(config_file)
        server_name = conf["azure_sql_server"]["server_name"]
        db_name = conf["azure_sql_server"]["db_name"]
        username = conf["azure_sql_server"]["username"]
        password = conf["azure_sql_server"]["password"]
                
    azure_db=AzureSQLDatabaseModule(server_name,db_name,username,password)
    return azure_db


def read_json_to_dataframe(file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
        df = pd.DataFrame(data)
        return df
    
def get_current_hour():
    # Get current date and time
    current_date = datetime.now()
    current_hour = current_date.hour
    current_minute = current_date.minute

    # Format the current date and time as strings
    date_string = current_date.strftime('%Y-%m-%d')
    time_string = f"{str(current_hour).zfill(2)}:{str(current_minute).zfill(2)}:00"  # Seconds are set to zero

    # Concatenate the date and time strings
    result_string = date_string + ' ' + time_string
    return result_string

