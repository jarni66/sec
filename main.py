import asyncio
import sys
import traceback
from clincal_reasoning import ClinicalDebate
import gcs_operation
import discovery
import config
import db_ops   
import enrich
import requests
from datetime import datetime
import json
import pandas as pd
import uuid
import config
import time
from concurrent.futures import ThreadPoolExecutor




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


if __name__ == "__main__":
    try:
        command = sys.argv[1]
        raw_args = sys.argv[2:]
        args = parse_key_value_args(raw_args)

    
        print(f"▶️ Running command: {command} with args: {args}")
        if command == "debate_clinical":
            print("Running clinical debate")

        else:
            print(f"❌ Unknown command: {command}")
            sys.exit(1)
    except Exception:
        err = traceback.print_exc()
        print(err)
        sys.exit(1)


