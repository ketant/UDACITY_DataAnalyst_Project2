# Name : prj2_visualization.Py 
# Created By : Tan Kwok Wee
# Purpose : Querying mongodb and create a histogram
# 1. Extract counts of entries group by hour
# 2. Contrusct histogram
# 3. Display histogram

from pymongo import MongoClient
from ggplot import *
import pprint
import pandas

# function get_db() was created to get db connection to local mongo db database

def get_db():
    client = MongoClient('localhost:27017')
    db = client.mydb
    return db


# function dot_query() returns the query string required to query the mondodb
	
def dot_query():
    query = [ {"$project" : {"hourOfDay" : {"$hour" : "$created.timestamp"}}}
              , {"$group":{"_id" : "$hourOfDay", "count" : {"$sum" : 1}}}
              , {"$sort" : {"_id" : 1}}]

    return query


if __name__ == "__main__":

    db = get_db()
    query = dot_query()
	
	# run the query and store the result in varaible creationByHour
    creationByhour = pandas.DataFrame(db.mydb.aggregate(query)['result'])
    
    print creationByhour
    #print db.mydb.count()

	# construct histogram	
    plot = ggplot(creationByhour, aes('_id','count')) + geom_bar(position = 'stack', stat = 'identity',width=0.5) + \
           ggtitle("Histogram - Creation of openstreetmapdata Hourly") + xlab("Hour (0-23)") + \
           ylab('Count of Creation') + \
           xlim(-1,24) + ylim(0,150000)     

    print plot
