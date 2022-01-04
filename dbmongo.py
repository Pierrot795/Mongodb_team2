import pymongo
from pymongo import MongoClient
from bson.json_util import ObjectId,loads,dumps
from dotenv import load_dotenv
import os
from datetime import datetime
from sshtunnel import SSHTunnelForwarder





def db_import():
    
    server = SSHTunnelForwarder(
    "mongodb009.westeurope.cloudapp.azure.com",
    ssh_username="administrateur",
    ssh_password="Mongodb13377",
    remote_bind_address=('mongodb009', 30000)
)
    server.start()
    client = MongoClient('127.0.0.1', server.local_bind_port) 
    db = client.grants
    return db

def timemeasure(collection,pipeline):
    time1=datetime.now()
    for i in range(10):
       collection.aggregate(pipeline)
       time2=datetime.now()
    res = (time2-time1)/10
    print(str(res))
    print()

#main_collection
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

#main_collection
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


#award_investigators
def query4(first_name,last_name,email_id,db):
    return loads(dumps(db.award_investigators.find({ 
        "investigators.first_name" : first_name, 
        "investigators.last_name" : last_name, 
        "investigators.email_id" : email_id
    }, 
    { 
        "award_title" : 1.0
    })))


#foa_info_awards
def query3(name,db):
    return loads(dumps(db.foa_info_awards.find(    { 
        "name" : name
    }, 
    { 
        "w_award.award_title" : 1.0, 
        "w_award.award_expiration_date" : 1.0
    })))

#main_collection
def query5(db):
    pipeline = [
        {
            "$unwind": {
                "path": "$awards",
            
            }
        },

        {
            "$group": {
                "_id": '$name',
                "averageAmount":{"$avg":'$awards.award_amount'},
                "count":{"$sum":1},
            
            }
        },


        {
            "$match": {
                "count":{"$gte":8}
                
            }
        },


        {
            "$project": {
                'name':1,
                'averageAmount':1,
                'count':1
                
            }
        }
    ]
    return loads(dumps(db.main_collection.aggregate(pipeline)))

#organisation_awards
def query7(date1,date2,db):
    pipeline = [
        {
            "$unwind": {
            "path": '$w_award'
            }
        },

        {
            "$match": {
                'w_award.award_effective_date':{"$gte":date1},
               'w_award.award_expiration_date':{"$lte":date2}
                
            }
        },


        {
            "$group": {
                "_id": '$code',
                "averageAmount":{"$avg":'$w_award.award_amount'},
                "awards":{'$addToSet':"$w_award"}
            }
        },


        {
            "$project": {
               'awards.award_title':1,
               'awards.award_amount':1,
               'averageAmount':1
                
            }
        }
    ]
    return loads(dumps(db.organisation_awards.aggregate(pipeline)))

#main_collection
def query6(date1,date2,db):
    pipeline = [
        {
            "$unwind": {
                
                "path":"$awards"
            }
        },


        {
            "$match": {
               'awards.award_effective_date':{"$gte":date1},
               'awards.award_expiration_date':{"$lte":date2}
                
            }
        },


        {
            "$group": {
                "_id": '$name',
                "sumAmount":{"$sum":'$awards.award_amount'}
            }
        },


        {
            "$sort": {
                'sumAmount':-1
            }
        },


        {
            "$limit": 10
        },
    ]

    return loads(dumps(db.main_collection.aggregate(pipeline)))


#award_investigators
def query8(db):
    pipeline = [
		{"$sort":{'award_effective_date':-1}}, 


		{
			"$limit": 1000
		},


		{
			"$unwind": {
			    "path": '$investigators'
			}
		},


		{
			"$group": {
			    "_id": ['$investigators.email_id','$investigators.first_name','$investigators.last_name'],
			    "maxAmount":{"$max":'$award_amount'}
			}
		},


		{
			"$sort": {
			    'maxAmount':-1
			    
			}
		},


		{
			"$project": {
			    'maxAmount':1
			    
			}
		}
	]
    return loads(dumps(db.award_investigators.aggregate(pipeline)))