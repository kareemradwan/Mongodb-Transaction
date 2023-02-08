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

"""
You can't execute `run_transaction()` inside `function` 
"""


def run_session_transaction(event, context, function):
    _session = client.start_session()
    try:

        _session.start_transaction()
        response = function(event, context, _session)
        try:
            _session.commit_transaction()
        except:
            pass
        return response.result()

    except ValueError as ex:
        if _session.in_transaction:
            _session.abort_transaction()
        raise ex
    except PyMongoError as ex:
        if _session.in_transaction:
            _session.abort_transaction()
        raise ex
    except Exception as ex:
        if _session.in_transaction:
            _session.abort_transaction()
        raise ex
    finally:
        _session.end_session()


"""
    you can run `run_transaction()` multiple times inside the `function`

    Must Return: Response
    @:return Response
"""


def run_session(event, context, function):
    _session = client.start_session()
    try:
        response = function(event, context, _session)
        return response.result()
    except Exception as ex:
        if _session.in_transaction:
            _session.abort_transaction()
        raise ex
    finally:
        _session.end_session()


"""
    will return object data not Response
    for example [] , {}, "OK"
"""


def run_transaction(_session: ClientSession, event, context, function):
    try:
        _session.start_transaction()
        _result = function(event, context, _session)
        _session.commit_transaction()
        return _result
    except Exception as ex:
        print("exception: " + ex.__str__())
        _session.abort_transaction()
        raise ex


def lock_document(collection: Collection, document_id, session: ClientSession, lock_time=30):
    collection.update_one(
        {"_id": document_id},
        {"$set": {"lock_timestamp": datetime.now() + timedelta(seconds=lock_time)}},
        upsert=False,
        session=session
    )
