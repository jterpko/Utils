#!/opt/python/bin/python 
from __future__ import print_function
import datetime
import sys
import json
from bson import BSON
from optparse import OptionParser
from bson.objectid import ObjectId
from pymongo import Connection, ReadPreference
from pymongo.errors import ConnectionFailure, OperationFailure



parser = OptionParser()
parser.add_option("-H", "--host", dest="host", 
    help='Mongo(s) to connect to', metavar="host")
parser.add_option("-p", "--port", type=int, dest="port",
    help='Mongos Port', metavar="port")
parser.add_option("-d", "--database", dest="db",
    help='Database to Profile', metavar="db")
parser.add_option("-u", "--user", dest="user",
    help='User for auth', metavar="user")
parser.add_option("-P", "--password", dest="password",
    help='Password for auth', metavar="password")
parser.add_option("-a", "--authdb", dest="authdb",
    help='DB to auth against , uses -d if not set', metavar="authdb")
parser.add_option("-n", "--noAuth", action="store_true",dest="noauth",
    help='DB to auth against , uses -d if not set', metavar="noauth")
(options, args) = parser.parse_args()

#Option validation Phase
if options.port == None or options.host == None:
    raise SystemExit("ERROR - You must have mongo host and port to run!")
if options.noauth == None:
    if options.authdb == None:
        options.authdb = options.db

#Build Mongo connection to main
client = Connection(options.host, int(options.port));
if options.noauth == None:
    try:
        if options.user != None and options.password != None:
            db = client[options.authdb];
            db.authenticate(options.user,options.password)
    except Exception,e:
        print("Username/Password error: please check your values, and set -n if you want to skip auth")
        sys.exit(1)

db = client[options.db]
SummaryTable={}
profiler = db.system.profile.find()
try:
    for item in profiler:

        if 'query' in item:
            query = item['query']
            if '$query' in query:
                query = query['$query']
        else:
            continue
        db = item['ns']
        if db not in SummaryTable:
            SummaryTable[item['ns']] = {}
        
        #queryPattern = ",".join(query.keys())
        tKeys = []
        for key in query:
            if key.startswith("$"):
		tKeys.append(query[key].keys())
            else:
                tKeys.append(key)
        queryPattern=",".join(tKeys)        
	
        if queryPattern not in SummaryTable[db]:
            SummaryTable[db][queryPattern]={}

        if 'count' in SummaryTable[db][queryPattern]:
            SummaryTable[db][queryPattern]['count'] += 1
        else:
            SummaryTable[db][queryPattern]['count'] = 1
        
        
        if 'nscanned' in SummaryTable[db][queryPattern]:
            if 'nscanned' in item:
                SummaryTable[db][queryPattern]['nscanned'] += item['nscanned']
            else:
                pass
        else:
            SummaryTable[db][queryPattern]['nscanned'] = 0 

        if 'nreturned' in SummaryTable[db][queryPattern]:
            if 'nreturned' in item:
                SummaryTable[db][queryPattern]['nreturned'] += item['nreturned']
            else:
                pass
        else:
            SummaryTable[db][queryPattern]['nreturned'] = 0 
except Exception as ex:
    print(ex)

print(json.dumps(SummaryTable, indent=4))


