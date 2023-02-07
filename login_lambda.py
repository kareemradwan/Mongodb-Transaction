import json
import os
import logging
import pdb
import uuid
from datetime import datetime

jwt_secret = os.getenv('JWT_SECRET')
default_signup_sku = os.getenv('DEFAULT_SIGNUP_SKU')

logger = logging.getLogger()
from abstract_transaction_session import *


def execute(event, context):
    return run_session_transaction(event, context, _execute)


def _execute(event, context, session):
    try:

        # raise  Exception("test-1")

        if 'username' not in event or event['username'] is None:
            return Response(status_code=400, body=json.dumps({"message": "username is a mandatory field"}))

        if 'password' not in event or event['password'] is None:
            return Response(status_code=400, body=json.dumps({"message": "password is a mandatory field"}))

        users_collection = db.get_collection("users_collection")

        user = users_collection.find_one({
            "username": event['username'],
            "password": event["password"]
        }, session=session)

        if user is None:
            return Response(status_code=401, body=json.dumps({"message": "Wrong username or password"}))

        user["_id"] = str(user['_id'])
        events_collection = db.get_collection("events_collection")
        events_collection.insert_one({
            "type": "login",
            "status": True
        })

        return Response(status_code=200, body=json.dumps(user))

    except Exception as ex:
        return Response(400, json.dumps({"message": "General Exception Occur " + ex.__str__()}))


response = execute({
    "username": "kradwan",
    "password": "kradwan"
}, None)

print(response.__str__())