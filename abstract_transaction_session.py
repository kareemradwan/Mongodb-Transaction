import json
from datetime import datetime, timedelta
from pymongo.client_session import ClientSession
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
import logging

from response import Response

_uriString = 'mongodb://localhost:27017/'
client = MongoClient(_uriString)
db = client.get_database("test-db")

_logger = logging.getLogger()


def run_session_transaction(event, context, function):
    _session = client.start_session()

    try:

        _session.start_transaction()
        response = function(event, context, _session)
        _session.commit_transaction()
        return response.result()

    except ValueError:
        if _session.in_transaction:
            _session.abort_transaction()
        return Response(status_code=400, body=json.dumps({"message": "ValueError Exception  "})).result()
    except PyMongoError as ex:
        if _session.in_transaction:
            _session.abort_transaction()
        return Response(status_code=400, body=json.dumps({"message": "PyMongoError Exception  "})).result()
    except Exception as ex:
        if _session.in_transaction:
            _session.abort_transaction()
        return Response(status_code=400, body=json.dumps({"message": ex.__str__()})).result()
    finally:
        _session.end_session()


def run_transaction(session: ClientSession, event, context, function):
    try:
        session.start_transaction()
        response = function(event, context, session)
        session.commit_transaction()
        return response
    except ValueError as ex:
        if session.in_transaction:
            session.abort_transaction()
        return Response(status_code=400, body=json.dumps({"message": "ValueError Exception  "})).result()
    except PyMongoError as ex:
        if session.in_transaction:
            session.abort_transaction()
        return Response(status_code=400, body=json.dumps({"message": "PyMongoError Exception  "})).result()
    except Exception as ex:
        if session.in_transaction:
            session.abort_transaction()
        return Response(status_code=400, body=json.dumps({"message": ex.__str__()})).result()


def lock_document(collection: Collection, document_id, session: ClientSession, lock_time=30):
    collection.update_one(
        {"_id": document_id},
        {"$set": {"lock_timestamp": datetime.now() + timedelta(seconds=lock_time)}},
        upsert=False,
        session=session
    )
