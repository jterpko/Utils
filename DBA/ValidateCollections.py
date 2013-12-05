#!/usr/bin/env  python
from __future__ import print_function
from pymongo import MongoClient
from pymongo import ReadPreference
from bson import BSON
import json
from optparse import OptionParser
import time
import sys, traceback
from collections import OrderedDict
import re


def checkRequiredArguments(opts, parser):
  missing_options = []
  for option in parser.option_list:
    if re.match(r'^\[REQUIRED\]', option.help) and eval('opts.' + option.dest) == None:
      missing_options.extend(option._long_opts)
  if len(missing_options) > 0:
    print('Missing REQUIRED parameters: ' + str(missing_options))
    parser.print_help()
    exit(-1)


def mongo_connect(options,dbname,return_client=False):
  client      = MongoClient(options.host, int(options.port))
  dbconnector = client['admin']
  dbconnector.authenticate(options.user,options.password)
  if return_client == False:
    dbconnector = client[dbname]
  else:
    dbconnector = client
  return  dbconnector
def validate_data(options):
    _dbconnector = mongo_connect(options,'admin',True)
    databases_to_process=[]
    if not options.database_name:
      for db in _dbconnector.database_names():
        if db not in ['admin','local','test']:
          databases_to_process.append(db)
        else:
          print("Skipping datbase",db)
    else:
      databases_to_process.append(options.database_name) 
    for database in databases_to_process:
      dbconnector = _dbconnector[database]
      print("Processing for database",database,":")
      try:
        collections  = dbconnector.collection_names()
      except:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    
      for coll in collections:
        validate_result=dbconnector.validate_collection(coll,full=True,scandata=True)
        if not validate_result['valid']:
          print("\t",coll,"had a failed validation of",validate_result['errors'])
        else:
  	      print("\t",coll," was OK")


def parserSetup():
  parser = OptionParser(version="%prog 0.1-alpha")
  parser.add_option(
    "-H", "--host", 
    dest="host",
    help="[REQUIRED] Mongos host", 
    type="string",  
  )
  parser.add_option(
    "-P", "--port", 
    type="int",
    dest="port",
    help="[REQUIRED] Mongos Port", 
  )
  parser.add_option(
    "-d", "--db-name", 
    type="string", 
    dest="database_name",
    default=False,
    help="which database to check", 
  )
  parser.add_option(
    "-u", "--username", 
    type="string",
    dest="user",
    help="[REQUIRED] Admin Auth User", 
  )
  parser.add_option(
    "-p", "--password", 
    type="string",
    dest="password",
    help="[REQUIRED] Admin Auth Password", 
  )
  return parser


 
if __name__ == "__main__":
  parser = parserSetup()
  (options, args) = parser.parse_args()
  checkRequiredArguments(options,parser)
  validate_data(options)
