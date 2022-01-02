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


def query4(first_name,last_name,email_id,db):
    return loads(dumps(db.award_investigators.find({ 
        "investigators.first_name" : first_name, 
        "investigators.last_name" : last_name, 
        "investigators.email_id" : email_id
    }, 
    { 
        "award_title" : 1.0
    })))

def query3(name,db):
    return loads(dumps(db.foa_info_awards.find(    { 
        "name" : name
    }, 
    { 
        "w_award.award_title" : 1.0, 
        "w_award.award_expiration_date" : 1.0
    })))

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

def query7(date1,date2,db):
    return loads(dumps(db.organisation_awards.aggregate([
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
    ])))

def query6(date1,date2,db):
    return loads(dumps(db.main_collection.aggregate([
        {
            "$unwind": {
                
                "path":"$awards"
            }
        },


        {
            "$match": {
               'awards.award_effective_date':{"$gte":'01/01/2000'},
               'awards.award_expiration_date':{"$lte":'01/01/2010'}
                
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
    ])))


def query8(db):
    return loads(dumps(db.award_investigators.aggregate([
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
	])))