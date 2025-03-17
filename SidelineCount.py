import json
import requests
import csv
import threading
import time
import pandas as pd
from datetime import datetime
import os

x = datetime.now()
datestr = x.strftime("%d") + x.strftime("%b").lower()
outdir = 'partyid_list' + '_' + datestr + '.csv'
outdir1 = 'count' + '_' + datestr + '.csv'


queue_list = ['gal_invoice_shovel_ingestion_shadow','apl_accrual_service_europa_fkmp_accrual_agg_1_production']


hdrs = dict()
thread_count_limit = 0
sleep_time = 1  # if you reduce this, it's possible that group_id_list will be empty need to check why
is_break = 0
limit = 1000
outbound_table = None
varadhi_endpoint = "http://10.24.0.209"  # Updated Endpoint after Podium introduction
total_dump_count = 0
party_list = []
def generate_queue_token():
    auth_url = 'https://service.authn-prod.fkcloud.in/oauth/token'
    payload = {
        'grant_type': "client_credentials",
        'client_id': "fk-ws-clairvoyant",
        'client_secret': "prsyku+kjWTtxRkZTpl+tw2YaWPfEOd5V0+o1MXjfq3p1CUJ",
        'target_client_id': "http://10.24.0.209:80"
    }
    headers = {
        'content-type': "application/x-www-form-urlencoded"
    }
    response = requests.request("POST", auth_url, data=payload, headers=headers)
    data = json.loads(response.content.decode())
    # print(data)
    return str(data["access_token"])

count=[]
def get_queue_count(queue_name):
    link = varadhi_endpoint + "/queues/" + queue_name + "/messages/actions/count?type=sideline"
    access_token = generate_queue_token()
    hdrs['Authorization'] = 'Bearer '+access_token
    resp = requests.get(link, headers=hdrs)
    if int(resp.text) > 0:
        d = queue_name,int(resp.text)
        count.append(d)
    with open(outdir1,'w',newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['queue_name','count'])
        writer.writerows(count)
        queue_count = int(resp.text)
        thread_control_count = 0
        threads = []
        offset = 0
        no_of_threads = int(queue_count / limit) + 1
        # print(no_of_threads)
        if no_of_threads == 0:
            no_of_threads = 1
        for i in range(no_of_threads):
            thread_control_count = thread_control_count + 1
            t = threading.Thread(target=get_message_dump,
                                 args=(queue_name, offset, offset + limit, 0))
            offset = offset + limit
            threads.append(t)
            t.start()
            if thread_control_count > thread_count_limit:
                time.sleep(sleep_time)
                thread_control_count = 0
                if is_break == 1:
                    break

def get_message_dump(queue_name, start, offset,retry_count):
    while start < offset:
        link = varadhi_endpoint + "/queues/" + queue_name + "/messages?sidelined=true" + "&offset=" + str(
            start) + "&limit=" + str(limit)
        start = start + limit
        try:
            resp = requests.get(link, headers=hdrs)
            if resp.status_code == 200 :
                resp_body = resp.json()
                for messages in resp_body:
                    response_body = str(json.dumps(messages.get('http_response_body')))
                    if str(response_body) != 'null':
                        if response_body.find("[party_id_from] is [") != None and response_body.find("[party_id_from] is [") != -1:
                            aa = response_body.find("[party_id_from] is [")+20
                            bb = response_body.find("] is not valid")
                            party_id = response_body[aa:bb].strip()
                            party_list.append([party_id])
                            # print(party_id)
            with open(outdir, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['party_id'])
                writer.writerows(party_list)
        except Exception as e:
            print(None)


if __name__ == "__main__":
    cwd = os.getcwd()
    directory = cwd
    # print(os.listdir(directory))
    extension = ".csv"
    for filename in os.listdir(directory):
        if filename.endswith(extension):
            # print(filename.endswith(extension))
            os.remove(os.path.join(directory, filename))


    for queue_name in queue_list:
        get_queue_count(queue_name)
    print("completed_list search")
    time.sleep(60)
    df = pd.read_csv(outdir)
    # print(df)
    df = df['party_id'].drop_duplicates()
    df.to_csv(outdir.split('.')[0] + '_op.csv',index=False)
