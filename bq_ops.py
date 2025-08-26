from pandas_gbq import read_gbq
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import gcs_ops
import pandas_gbq

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



def upload_to_bq(args):
    batch_url = f"gs://financial_data_nizar/run_log/{args.batch_name}"
    all_acsn = get_all_acsn()

    uploaded_cik_form_path = f"gs://financial_data_nizar/run_log/BATCH_LOG/{args.batch_name}_upload.json"

    try:
        processed_cik_form = gcs_ops.read_json_from_gcs(uploaded_cik_form_path)
    except:
        processed_cik_form = []

    cik_form_reports = gcs_ops.list_gcs_children(batch_url)
    unprocessed_cik = [i for i in cik_form_reports if i not in processed_cik_form]

    df_add = pd.DataFrame()


    for cik_form in unprocessed_cik:
        df_form = pd.read_csv(cik_form)
        df_form = df_form[df_form['bq_status'] == 'process']

        for i, row in df_form.iterrows():
            row_acsn = row['accessionNumber']
            if row_acsn not in all_acsn:
                file_path = row['upload_to_bucket_status']
                if file_path.startswith('gs'):
                    df_par = pd.read_parquet(file_path)
                    df_add = pd.concat([df_add, df_par], axis=0)

        if len(df_add) > 500000:
            pandas_gbq.to_gbq(
                df_add,
                destination_table=table_id,
                project_id=project_id,
                if_exists='append',  # 'replace', 'fail', or 'append' (append new data)
            )
            print(f'APPENDED', len(df_add))
            df_add = pd.DataFrame()

        processed_cik_form.append(cik_form)
        gcs_ops.write_or_update_json_to_gcs(uploaded_cik_form_path, processed_cik_form)

    if len(df_add) != 0:
        pandas_gbq.to_gbq(
            df_add,
            destination_table=table_id,
            project_id=project_id,
            if_exists='append',  # 'replace', 'fail', or 'append' (append new data)
        )
        print(f'APPENDED', len(df_add))
        df_add = pd.DataFrame()