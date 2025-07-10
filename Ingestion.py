import sys
import requests
import csv
import json
from collections import defaultdict
from datetime import date

def get_group_id(entity):
    return entity + "_" + date.today().strftime("%Y%m%d")

def change_case(str):
    res = [str[0].lower()]
    for c in str[1:]:
        if c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
            res.append('_')
            res.append(c.lower())
        else:
            res.append(c)

    return ''.join(res)

def get_ids():
    ids = []
    with open("txn_upload.csv" , 'r') as f:
        reader = csv.reader(f, dialect='excel', delimiter=',')
        for line in reader:
            ids.append(line[0])
    return ids

def get_rows(file_name):
    rows = []
    with open(file_name , 'r') as f:
        reader = csv.reader(f, dialect='excel', delimiter=',')
        for line in reader:
            rows.append(line)

    return rows

def grouped_rows(rows):
    res = defaultdict(list)
    for row in rows:
        res[row[0]].append(row[1:])
    return res

def ingest(payload, headers):
    print(json.dumps(payload))
    url = "http://10.24.5.199/v1/ingestionEntities/bulk"
    response = requests.post(url, json.dumps(payload), headers=headers)
    return response

def ingest_last_batch(payload, context_arr, headers, count):
    if len(context_arr) != 0:
        payload["context"] = context_arr
        response = ingest(payload, headers)
        print(count)
        print(response)
        del(context_arr)

def ingest_invoices(bu_id, inv_type, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "invoice_by_id"
    payload["group_id"] = get_group_id("invoice_by_id2")
    payload["headers"] = {
        "X_BU_ID": bu_id,  
        "X_INVOICE_TYPE": change_case(inv_type)
    }

    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "type": change_case(inv_type),
            "id": item[0]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            print(count)
            response = ingest(payload, headers)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_invoice_by_client_ref_id(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "invoice_by_client_ref_id"
    payload["group_id"] = get_group_id("ainvoice_by_client_ref_id")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "type": change_case(item[0]),
            "client_ref_id": item[1]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            print(count)
            response = ingest(payload, headers)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_transactions(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "payment_advisor_transaction"
    payload["group_id"] = get_group_id("apayment_advisr_transaction")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "headerId": item[0],
            "transactionId": item[1]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_i2p(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "i2p_application_by_id"
    payload["group_id"] = get_group_id("ai2p")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "type": "i2p_application",
            "id": item[0]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_payment(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "payment"
    payload["group_id"] = get_group_id("apayment")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "type": change_case(item[0]),
            "clientRefId": item[1]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_advice(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "payment_advisor"
    payload["group_id"] = get_group_id("aadvice")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "id": item[0]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_accrual(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "accrual_by_id"
    payload["group_id"] = get_group_id("accrual")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "type": change_case(item[0]),
            "id": item[1]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_accrual_by_client_ref_id(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "accrual_by_client_ref_id"
    payload["group_id"] = get_group_id("accrual_by_client_ref_id")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "type": change_case(item[0]),
            "client_ref_id": item[1]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_bank_txns(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "bank_statement_transaction"
    payload["group_id"] = get_group_id("bank_statement_txn")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "transaction_id": change_case(item[0])
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_payment_mapping(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "invoice_payment_mapping"
    payload["group_id"] = get_group_id("apayment_mapping")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "client_ref_id": item[0]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_groot_disbursement(bu_id, items, batch):
    count = 0
    payload = {}
    payload["entity_name"] = "disbursement"
    payload["group_id"] = get_group_id("groot")
    payload["headers"] = {
        "X_BU_ID": bu_id
    }
    headers = {"Content-Type": "application/json", "reingestion":"true"}

    context_arr = []
    for item in items:
        count += 1
        context = {
            "transaction_id": item[0]
        }
        context_arr.append(context)

        if count % batch == 0:
            payload["context"] = context_arr
            response = ingest(payload, headers)
            print(count)
            print(response)
            context_arr = []

    ingest_last_batch(payload, context_arr, headers, count)

def ingest_advice_advice_mapping(bu_id,items,batch):
    count =0
    payload={}
    payload["entity_name"] = "advice_advice_mapping"
    payload["group_id"]= get_group_id("advice_advice_mapping")
    payload["headers"] = {
            "X_BU_ID":bu_id
    }
    headers = {"Content-Type":"application/json","reingestion":"true"}

    context_arr=[]
    for item in items:
        count+=1
        context ={
                "id":item[0]
        }
        context_arr.append(context)
        if count % batch == 0:
            payload["context"]=context_arr
            response=ingest(payload,headers)
            context_arr= []

    ingest_last_batch(payload,context_arr,headers,count)

if len(sys.argv) != 3:
    print("ERROR: Bad command line")
    print("Usage: python ingest.py {file_name} {entity}")
    sys.exit()

entity = sys.argv[2]
file_name = sys.argv[1]
rows = get_rows(file_name)

if entity == "invoice":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        type_map = grouped_rows(det)
        for inv_type, inv_type_det in type_map.items():
            ingest_invoices(bu, inv_type, inv_type_det, 200)

elif entity == "invoice_by_client_ref_id":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_invoice_by_client_ref_id(bu, det, 500)

elif entity == "transactions":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_transactions(bu, det, 500)

elif entity == "i2p":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_i2p(bu, det, 200)

elif entity == "payment":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_payment(bu, det, 200)

elif entity == "advice":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_advice(bu, det, 200)

elif entity == "accrual":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_accrual(bu, det, 200)

elif entity == "payment_mapping":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_payment_mapping(bu, det, 200)

elif entity == "accrual_by_client_ref_id":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_accrual_by_client_ref_id(bu, det, 200)

elif entity == "bank_statement_transaction":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_bank_txns(bu, det, 200)

elif entity == "groot":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_groot_disbursement(bu, det, 200)

elif entity =="advice_advice_mapping":
    txn_map = grouped_rows(rows)
    for bu, det in txn_map.items():
        ingest_advice_advice_mapping(bu,det,500)

else:
    print("invoice\ninvoice_by_client_ref_id\ntransactions\ni2p\npayment\nadvice\naccrual\naccrual_by_client_ref_id\nbank_statement_transaction\npayment_mapping\ngroot")
