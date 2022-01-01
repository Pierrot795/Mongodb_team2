import pymongo
from pymongo import MongoClient
from bson.json_util import ObjectId,loads,dumps

def db_import():
    client = MongoClient('localhost', 27017) #remplacer par les id du mongos
    db = client.Test_grants
    return db

def query1(name,award_title,award_id,db):
    pipeline = [
		{
			"$match": {
			    'name':name
			}
		},
		{
			"$unwind": {
			    "path": '$awards'
			}
		},
		{
			"$match": {
			    'awards.award_title':award_title,
			    'awards.award_id':award_id
			    
			}
		},
		{
			"$project": {
			    'awards.investigators':1
			    
			}
		}
	]

    return loads(dumps(db.main_collection.aggregate(pipeline)))

def query2(name,db):
    return loads(dumps(db.main_collection.find(  { 
        "name" : name
    }, 
    { 
        "awards.award_title" : 1.0, 
        "awards.award_effective_date" : 1.0, 
        "awards.award_expiration_date" : 1.0, 
        "awards.award_amount" : 1.0, 
        "awards.organisation" : 1.0
    })))