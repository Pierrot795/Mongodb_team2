from dbmongo import db_import, query5
from datetime import datetime

db = db_import()

#main_collection
pipeline1 = [
		{
			"$match": {
			    'name':'18F GSA'
			}
		},
		{
			"$unwind": {
			    "path": '$awards'
			}
		},
		{
			"$match": {
			    'awards.award_title':'IIP Website Redesign Cloud.gov',
			    'awards.award_id':1708471
			    
			}
		},
		{
			"$project": {
			    'awards.investigators':1
			    
			}
		}
	]

#main_collection
pipeline2 = [{"$match":{ 
        "name" : "18F GSA"
    }}, 
    {"$project":{ 
        "awards.award_title" : 1.0, 
        "awards.award_effective_date" : 1.0, 
        "awards.award_expiration_date" : 1.0, 
        "awards.award_amount" : 1.0, 
        "awards.organisation" : 1.0
    }}]

#foa_info_awards
pipeline3 = [ {"$match":{ 
        "name" : "Physical Sciences"
    }}, 
    {"$project":{ 
        "w_award.award_title" : 1.0, 
        "w_award.award_expiration_date" : 1.0
    }}]

#award_investigators
pipeline4 = [{"$match":{ 
        "investigators.first_name" : "John", 
        "investigators.last_name" : "Ziegert", 
        "investigators.email_id" : "jziegert@uncc.edu"
    }}, 
    {"$project":{ 
        "award_title" : 1.0
    }}]

#main_collection
pipeline5 = [
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

#main_collection
pipeline6 = [
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
    ]

#organisation_awards
pipeline7 =  [
        {
            "$unwind": {
            "path": '$w_award'
            }
        },

        {
            "$match": {
                'w_award.award_effective_date':{"$gte":'01/01/2000'},
               'w_award.award_expiration_date':{"$lte":'01/01/2000'}
                
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

#award_investigators
pipeline8 = [
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

def timemeasure(collection,pipeline):
    time1=datetime.now()
    for i in range(10):
       collection.aggregate(pipeline)
       time2=datetime.now()
    res = (time2-time1)/10
    print("Mean time of execution: "+str(res))
    print()

print('query1:')
timemeasure(db.main_collection,pipeline1)
print('query2')
timemeasure(db.main_collection,pipeline2)
print('query3')
timemeasure(db.foa_info_awards,pipeline3)
print('query4')
timemeasure(db.award_investigators,pipeline4)
print('query5')
timemeasure(db.main_collection,pipeline5)
print('query6')
timemeasure(db.main_collection,pipeline6)
print('query7')
timemeasure(db.organisation_awards,pipeline7)
print('query8')
timemeasure(db.award_investigators,pipeline8)