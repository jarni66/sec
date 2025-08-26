from pandas_gbq import read_gbq
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

client = bigquery.Client()
client_storage = storage.Client()

project_id = 'nizar-financial-434904'
dataset_id = "financial_data"
bucket_name = "financial_data_nizar"
table_id = f'nizar-financial-434904.{dataset_id}.sec_forms'



def get_all_acsn():
    query = """
    SELECT DISTINCT acsn
    FROM `nizar-financial-434904.financial_data.sec_forms`;
        """
        
    project_id = 'nizar-financial-434904'

    df_acsn = read_gbq(query, project_id=project_id)
    return df_acsn





