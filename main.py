import asyncio
import sys
import traceback
import config
import requests
from datetime import datetime
import json
import pandas as pd
import uuid
import config
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import gcs_ops
import random
import sec.sec as sec
import traceback

# Convert key=value pairs into a dictionary
def parse_key_value_args(args):
    result = {}
    for arg in args:
        if "=" in arg:
            key, value = arg.split("=", 1)
            try:
                # Try parsing as JSON
                result[key] = json.loads(value)
            except json.JSONDecodeError:
                # If not JSON, keep as string
                result[key] = value
    return result


def run_sec(cik,batch_name):
    run_cik = sec.ProcessCIK(cik=cik, batch=batch_name)
    run_cik.run()
    time.sleep(1)

def run_batch(args):
    try:
        try:
            processed_cik = gcs_ops.read_json_from_gcs(f"run_log/BATCH_LOG/{args.get('batch_name')}_cik_process.json")
            if not processed_cik:
                processed_cik = {}
        except:
            processed_cik = {}
            
        processed_cik_list = []
        for k,v in processed_cik.items():
            processed_cik_list += v

        if not args.get('cik_list'):
            all_ciks = gcs_ops.read_json_from_gcs(config.CIK_PATH)
            cik_pools = [i.get('cik') for i in all_ciks]
        else:
            cik_pools = gcs_ops.read_json_from_gcs("run_log/BATCH_LOG/cik_list.json")
            if not cik_pools:
                cik_pools = []

        unprocessed_cik = [i for i in cik_pools if i not in processed_cik_list]
        print("Length unprocessed cik :",len(unprocessed_cik))
        picked_cik = random.sample(unprocessed_cik, int(args.get('batch_size')))

        object_status = {
            f"{args.get('batch_name')}_{args.get('batch_num')}" : picked_cik
        }

        gcs_ops.write_or_update_json_to_gcs(f"run_log/BATCH_LOG/{args.get('batch_name')}_cik_process.json", object_status)
        if args.get('mode') == 'concurrent':
            with ThreadPoolExecutor(max_workers=8) as executor:
                # Submit tasks
                futures = [executor.submit(run_sec, i, args.get('batch_name')) for i in picked_cik]
                
                for future in as_completed(futures):
                    result = future.result()
                    # print(f"Got: {result}")
        else:
            for cik in picked_cik:
                run_sec(cik, args.get('batch_name'))
    except:
        error = traceback.print_exc()
        print(error)


if __name__ == "__main__":
    try:
        command = sys.argv[1]
        raw_args = sys.argv[2:]
        args = parse_key_value_args(raw_args)

    
        print(f"▶️ Running command: {command} with args: {args}")
        if command == "run_sec":
            run_batch(args)
        else:
            print(f"❌ Unknown command: {command}")
            sys.exit(1)
    except Exception:
        err = traceback.print_exc()
        print(err)
        sys.exit(1)


