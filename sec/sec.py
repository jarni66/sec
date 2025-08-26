import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options as ChromeOptions
from datetime import datetime
import numpy as np
from google.cloud import bigquery
import re
import xml.etree.ElementTree as ET
import config


client = bigquery.Client()
BQ_TABLE = "nizar-financial-434904.financial_data.sec_forms"
FILLING_BUCKET = "gs://financial_data_nizar/filling_table"
RUN_LOG = "gs://financial_data_nizar/run_log"


headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ko;q=0.8,id;q=0.7,de;q=0.6,de-CH;q=0.5',
        'origin': 'https://www.sec.gov',
        'priority': 'u=1, i',
        'referer': 'https://www.sec.gov/',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'user-agent': 'Mario bot project resnatim@gmail.com',
    }



class ProcessCIK:
    def __init__(self, cik: str, batch: str):


        self.batch = batch
        self.cik = cik
        self.business_address = ''
        self.mailing_address = ''

        self.records = []

    def save_form(self):
        if self.records:
            df = pd.DataFrame(self.records)

            op_path = f"{RUN_LOG}/{self.batch}/{self.cik}.csv"
            df.to_csv(op_path, index=False)


    def cik_form(self):
        """
        This function use to get company forms data by using CIK.
        """

        session = requests.session()
        response = session.get(f'https://data.sec.gov/submissions/CIK{self.cik}.json', headers=headers)
        print("Get json forms", response.status_code)
        if response.status_code == 200:
            # If return success the result will in json format
            form_result = response.json()

            # State business and mailing address in empty string, in case there is no data.
            
            try:
                self.business_address = ', '.join([i for i in list(form_result['addresses']['business'].values()) if i ])
            except:
                pass
            
            try:
                self.mailing_address = ', '.join([i for i in list(form_result['addresses']['mailing'].values()) if i ])
            except:
                pass



            # Load json data to dataframe for easier iteration
            df_forms = pd.DataFrame(form_result['filings']['recent'])
            df_forms['cik'] = self.cik
            df_forms['business_address'] = self.business_address
            df_forms['mailing_address'] = self.mailing_address
            df_forms = df_forms[df_forms['form'].str.contains('13F')]
            
            records = df_forms.to_dict(orient="records")
            print(f"Length ({self.cik}) ACSN to process", len(records))
            for r in records:
                process_acsn = ProcessACSN(r)
                process_acsn.run()
                self.records.append(process_acsn.record)

        else:
            print(response.status_code)

    def run(self):
        self.cik_form()
        self.save_form()


class ProcessACSN:
    def __init__(self, row : dict):
        self.record = row
        self.acsn = self.record.get('accessionNumber')
        self.cik = self.record.get('cik')
        self.info_table_records = []

    def save_data(self):
        """
        Save result data to parquet file to the bucket
        """
        
        file_name = self.acsn + '.parquet'
        op_path = f"{FILLING_BUCKET}/{self.cik}/{file_name}"
        
        df = pd.DataFrame(self.info_table_records)
        
        df['business_address'] = self.record.get("business_address")
        df['mailing_address'] = self.record.get("mailing_address")
        df['report_date'] = self.record.get("reportDate")
        df['filling_date'] = self.record.get("filingDate")
        df['acsn'] = self.record.get("accessionNumber")
        df['cik'] = self.record.get("cik")
        df['value_multiplies'] = self.record.get("value_multiplies")
        df['scraping_url'] = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}.txt"
        df['scraping_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            # Checking column if not exist fill with nan
            for column, dtype in config.bq_dtype.items():
                if column not in df.columns:
                    df[column] = np.nan
            df = df.astype(config.bq_dtype)
            print(f"SAVED {op_path}", len(df))
            
            df.to_parquet(op_path, index=False)
            self.record['upload_to_bucket_status'] = op_path

            
        except Exception as e:
            error = f"Error in save_data : {e}"
            print(error)
            self.record['upload_to_bucket_status'] = error
            

    def check_acsn(self, acsn: str, table_id: str = BQ_TABLE) -> bool:
        """
        Check if the given ACSN exists in the BigQuery table.
        
        Args:
            acsn (str): The accession number to check.
            table_id (str): Full table ID in the form 'project.dataset.table'.
        
        Returns:
            bool: True if ACSN exists, False otherwise.
        """
        query = f"""
        SELECT 1
        FROM `{table_id}`
        WHERE acsn = @acsn
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("acsn", "STRING", acsn)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        return len(results) > 0
    
    def check_value_multiplies(self,url):
        response = requests.get(url, headers=headers)
        print("Check value multiplies",response.status_code)
        if response.status_code == 200:
            text_body = response.text
            match = re.search(r'x\$(\d+)', text_body)
            if match:
                return int(match.group(1))
            else:
                return 1
        else:
            return 1


    def get_html_info_table(self):
        url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}-index.html"
        response = requests.get(url, headers=headers)
        print("Get html info table url", response.status_code)
        if response.status_code == 200:
            text_body = response.text
            soup = BeautifulSoup(text_body, 'html.parser')
            table = soup.find('table')
            if table:
                all_tr = table.find_all('tr')
                for tr in all_tr:
                    a_tag = tr.find('a')
                    tr_str = str(tr).lower()
                    if ('table' in tr_str) and ('.html' in a_tag.text):
                        return "https://www.sec.gov" + a_tag.attrs.get('href')

    def parser(self, req_text):
        self.record['parser'] = 1
        match = re.search(r'(<[\w:]*informationTable[\s\S]*?</[\w:]*informationTable>)', req_text)
        if not match:
            return self.parser2(req_text)

        xml_block = match.group(1)

        # Parse XML
        root = ET.fromstring(xml_block)

        # Detect namespace (works for default or prefixed)
        def get_namespace(tag):
            if tag[0] == "{":
                return tag[1:].split("}")[0]
            return ""

        ns_uri = get_namespace(root.tag)
        ns = {"ns": ns_uri} if ns_uri else {}

        # Helper to build full tag path with namespace
        def ns_tag(tag_name):
            return f"ns:{tag_name}" if ns else tag_name

        data = []
        for info in root.findall(ns_tag("infoTable"), ns):
            entry = {
                "name_of_issuer": info.findtext(ns_tag("nameOfIssuer"), default="", namespaces=ns).strip(),
                "title_of_class": info.findtext(ns_tag("titleOfClass"), default="", namespaces=ns).strip(),
                "cusip": info.findtext(ns_tag("cusip"), default="", namespaces=ns).strip(),
                "figi": info.findtext(ns_tag("figi"), default="", namespaces=ns).strip(),
                "value": info.findtext(ns_tag("value"), default="", namespaces=ns).strip(),
                "shares_or_percent_amount": info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamt"), ns).text.strip() if info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamt"), ns) is not None else "",
                "shares_or_percent_type": info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamtType"), ns).text.strip() if info.find(ns_tag("shrsOrPrnAmt") + "/" + ns_tag("sshPrnamtType"), ns) is not None else "",
                "put_call": info.findtext(ns_tag("putCall"), default="", namespaces=ns).strip(),
                "investment_discretion": info.findtext(ns_tag("investmentDiscretion"), default="", namespaces=ns).strip(),
                "other_manager": info.findtext(ns_tag("otherManager"), default="", namespaces=ns).strip(),
                "voting_authority_sole": info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("Sole"), default="0", namespaces=ns).strip(),
                "voting_authority_shared": info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("Shared"), default="0", namespaces=ns).strip(),
                "voting_authority_none":info.findtext(ns_tag("votingAuthority") + "/" + ns_tag("None"), default="0", namespaces=ns).strip(),


            }
            data.append(entry)

        return data

    def parser2(self, req_text):
        self.record['parser'] = 2
        # Extract the <TABLE>...</TABLE> block
        # Extract the <TABLE> block
        match = re.search(r"<TABLE>([\s\S]*?)</TABLE>", req_text, re.IGNORECASE)
        if not match:
            raise ValueError("No <TABLE> block found")
        table_block = match.group(1)

        # Normalize: replace tabs with 2 spaces
        table_block = table_block.replace("\t", "  ")

        lines = table_block.strip().splitlines()
        data = []
        start_parsing = False

        for line in lines:
            # Start parsing after the "<S>" header line
            if "<C>" in line.strip():
                start_parsing = True
                continue
            if not start_parsing:
                continue
            if not line.strip():
                continue

            # Split fields by 2+ spaces
            parts = re.split(r"\s{2,}", line.strip())
            if len(parts) < 7:
                continue

            entry = {
                "name_of_issuer": parts[0],
                "title_of_class": parts[1],
                "cusip": parts[2],
                "figi": "",
                "value": parts[3],
                "shares_or_percent_amount": parts[4],
                "shares_or_percent_type": parts[5],
                "put_call":'',
                "investment_discretion": parts[6],
                "other_manager":'',
                "voting_authority_sole" : parts[7] if len(parts) > 7 else "",
                "voting_authority_shared":parts[8] if len(parts) > 8 else "",
                "voting_authority_none":parts[9] if len(parts) > 9 else "",


            }
            data.append(entry)

        return data



    def get_info_table(self):
        url_text = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}.txt"
        response = requests.get(url_text, headers=headers)
        print("Get info table full text", response.status_code)
        if response.status_code == 200:
            text_body = response.text

            info_table_records = self.parser(text_body)
            return info_table_records

    def run(self):
        if self.acsn:
            if not self.check_acsn(self.acsn):
                print("Process :",self.cik, self.acsn)
                self.record['bq_status'] = 'process'
                try:

                    url_html_info_table = self.get_html_info_table()
                    if url_html_info_table:
                        self.record['url_html_info_table'] = url_html_info_table
                        self.record['value_multiplies'] = self.check_value_multiplies(url_html_info_table)
                    else:
                        self.record['url_html_info_table'] = ""
                        self.record['value_multiplies'] = self.check_value_multiplies(f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{self.acsn.replace('-','')}/{self.acsn}.txt")

                    self.info_table_records = self.get_info_table()
                    self.record['info_table_len'] = len(self.info_table_records)
                    self.save_data()
                except Exception as e:
                    self.record['process'] = str(e)


            else:
                print("Exist :",self.cik, self.acsn)

                self.record['bq_status'] = 'exist'





        


