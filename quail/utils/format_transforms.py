import csv
import json

def csv_to_json(csv_str):
    reader = list(csv.DictReader(csv_str.splitlines()))
    return json.dumps(reader)
