#!/usr/bin/env python
# coding: utf-8

# # National Grid Data Pull

# ## Function to pull data from National Grid database using their API
# ### Requires the code for the dataset, start date, end data and max number of rows (typically 1 row = 30 min)

import pandas as pd
import requests
from urllib import parse

def data_pull_date(datacode,date_start,date_end,entry_limit):
    #Define Dataset and Timeframe to Download
    sql_query = '''SELECT COUNT(*) OVER () AS _count,
    * FROM "{}"
    WHERE "DATETIME" >= '{}'
    AND "DATETIME" <= '{}'
    ORDER BY "_id" ASC LIMIT {}'''.format(datacode,date_start,date_end,entry_limit)


    #Use National Grid API to Pull Data
    params = {'sql': sql_query}

    try:
        response = requests.get('https://api.nationalgrideso.com/api/3/action/datastore_search_sql',
                                params = parse.urlencode(params))
        data = response.json()["result"]
        df = pd.DataFrame(data["records"])   
        return df

    except requests.exceptions.RequestException as e:
        print(e.response.text)
        
def data_pull_all(datacode,entry_limit):
    #Define Dataset and Timeframe to Download
    sql_query = '''SELECT COUNT(*) OVER () AS _count,
    * FROM "{}"
    ORDER BY "_id" ASC LIMIT {}'''.format(datacode,entry_limit)

    
    #Use National Grid API to Pull Data
    params = {'sql': sql_query}

    try:
        response = requests.get('https://api.nationalgrideso.com/api/3/action/datastore_search_sql',
                                params = parse.urlencode(params))
        data = response.json()["result"]
        df = pd.DataFrame(data["records"])   
        return df

    except requests.exceptions.RequestException as e:
        print(e.response.text)



