import pymongo
from abstract_transaction_session import *



_my_collecitons = ["users_collection", "events_collection", "accounts_collection"]
_collections = db.list_collection_names()

print("Check Collections ... ")
for _collection in _my_collecitons:
    if _collection not in _collections:
        db.create_collection(_collection)
print("Collections Checked Passed 100% ")

users_collection = db.get_collection("users_collection")
accounts_collection = db.get_collection("accounts_collection")
events_collection = db.get_collection("events_collection")


users_collection.delete_many({})
accounts_collection.delete_many({})
events_collection.delete_many({})

users_collection.insert_one({
    'username': 'kradwan',
    'password': 'kradwan'
})
accounts_collection.insert_one({
    'name': 'Karem Account',
    'balance': 500
})

accounts_collection.insert_one({
    'name': 'Ahmed Account',
    'balance': 500
})

events_collection.insert_one({
    "event": "Login",
    "status": True
})

users_collection.create_index([('username', pymongo.ASCENDING)], name='users_username_index')


result =  users_collection.find({
    'username' : 'kradwan'
}, ).explain()

print(json.dumps(result , default=str , indent=4))