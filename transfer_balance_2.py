import json
import os
import logging
import pdb
import time
import uuid
from datetime import datetime

from bson import ObjectId

jwt_secret = os.getenv('JWT_SECRET')
default_signup_sku = os.getenv('DEFAULT_SIGNUP_SKU')

logger = logging.getLogger()
from abstract_transaction_session import *


def execute(event, context):
    return run_session_transaction(event, context, _execute)


def _execute(event, context, session):
    try:

        if 'from' not in event or event['from'] is None:
            return Response(status_code=400, body=json.dumps({"message": "from is a mandatory object"}))

        if 'id' not in event['from'] or event['from']['id'] is None:
            return Response(status_code=400,
                            body=json.dumps({"message": "please, pass id variable inside from object "}))

        if 'to' not in event or event['to'] is None:
            return Response(status_code=400, body=json.dumps({"message": "from is a mandatory object"}))

        if 'id' not in event['to'] or event['to']['id'] is None:
            return Response(status_code=400,
                            body=json.dumps({"message": "please, pass id variable inside to object "}))

        if 'amount' not in event or event['amount'] is None:
            return Response(status_code=400, body=json.dumps({"message": "amount is a mandatory object"}))

        if 'value' not in event['amount'] or event['amount']['value'] is None:
            return Response(status_code=400,
                            body=json.dumps({"message": "please, pass value variable inside amount object "}))

        accounts_collection = db.get_collection("accounts_collection")

        from_id = event['from']['id']
        to_id = event['to']['id']
        amount = event['amount']['value']

        lock_document(accounts_collection, ObjectId(from_id), session)
        lock_document(accounts_collection, ObjectId(to_id), session)

        accounts_collection.update_one({
            "_id": ObjectId(from_id)
        }, {
            "$inc": {
                "balance": amount
            }
        }, session=session)

        accounts_collection.update_one({
            "_id": ObjectId(to_id)
        }, {
            "$inc": {
                "balance": amount
            }
        }, session=session)

        events_collection = db.get_collection("events_collection")
        events_collection.insert_one({
            "type": "transfer_balance",
            "status": True
        }, session=session)
        return Response(status_code=200,
                        body=json.dumps({"status": True, "message": "transfer2 balance finished successfully"}))

    except PyMongoError as ex:
        raise Exception("PyMongoError Exception Occur in Account Module")
    except Exception as ex:
        raise Exception("General Exception Occur in Account Module")



account1 = '63e25b545c73d8ea1c571260'
account2 = '63e25b8f5c73d8ea1c571264'

event = {
    "from": {
        "id": account1
    },
    "to": {
        "id": account2
    },
    "amount": {
        "value": 50
    }
}

response = execute(event, None)
print(response)
