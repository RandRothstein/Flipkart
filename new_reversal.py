import json
import uuid
import sys
import requests
import pandas as pd


def postCall(payload, auth_key):
    url = "http://10.24.0.208:80/queues/ws_adjustment_framework/messages"
    # url = "http://10.24.0.208:80/queues/test_gal_europa/messages"
    message_id = str(uuid.uuid4())
    headers = {
        'Authorization': "Bearer " + auth_key,
        'X_RESTBUS_HTTP_URI': 'http://10.24.42.27:80/adjustment/create',
        'X_RESTBUS_GROUP_ID': message_id,
        'X_RESTBUS_MESSAGE_ID': message_id,
        'Content-Type': 'application/json',
        'X_RESTBUS_HTTP_METHOD': 'POST',
        'X_BU_ID': 'FKMP',
        'X_USER_ID': "akbar.shaikh"  # put your name
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.json()["X_RESTBUS_MESSAGE_ID"])


def get_auth_key():
    url='https://service.authn-prod.fkcloud.in/v3/oauth/token'
    # url = 'http://service.authn-prod.fkcloud.in/oauth/token' old Url
    payload = {
        'grant_type': "client_credentials",
        'client_id': "accounting-adjustment-framework",
        'client_secret': "Fp0qY4CriHP3rFK4cMOE9EJAyVaWV+Eg+JeUOvYjq5Egb1KD",
        'target_client_id': "http://10.24.36.251:80"
    }
    headers = {
        'content-type': "application/x-www-form-urlencoded"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    data = response.json()
    return data["access_token"]


def build(cref, etype, ticket_id, recipe, access_token):
    attributes_dict = {}
    attributes_array = []
    payload_dict = {}
    attributes_dict["client_ref_id"] = cref
    attributes_dict["type"] = etype
    attributes_dict["ticket_id"] = ticket_id
    attributes_array.append(attributes_dict)
    payload_dict["recipe_type"] = recipe
    payload_dict["attributes"] = attributes_array
    payload = json.dumps(payload_dict, indent=4)
    postCall(payload, access_token)


if __name__ == '__main__':
    df = pd.read_csv(sys.argv[1])
    client_ref_id = df['client_ref_id'].values.tolist() #invoice client ref id
    entity_type = df['type'].values.tolist() # invoice type
    ticket_id = "LCRED-1710"
    recipe = "invoice_reversal" #df['recipe'].values.tolist() # recipe to be used(invoice_reversal/accrual_reversal)

    access_token = get_auth_key()
    payList = []
    for i in range(len(entity_type)):
        build(str(client_ref_id[i]), str(entity_type[i]), str(ticket_id), str(recipe), access_token)
