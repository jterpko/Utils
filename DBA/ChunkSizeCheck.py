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



parser = OptionParser(version="%prog 0.1-alpha")
parser.add_option("-H", "--host", dest="host",
                  help="Mongos host", metavar="HOST")
parser.add_option("-P", "--port", dest="port",
                  help="Mongos Port", metavar="PORT")
parser.add_option("-d", "--db-name", dest="database_name",
                  help="which database to check", metavar="DATABASE_NAME")
parser.add_option("-c", "--collection-name", dest="collection_name",
                  help="which collection to check", metavar="COLLECTION_NAME")
####Adding Soon####
#parser.add_option("-R", "--readOnly", dest="readOnly",
#                  help="ReadOnly: Do not split chunks", metavar="ReadOnly")
#parser.add_option("-n", "--number_chunks", dest="chunk_count",
#                  help="Number of chunks to split (when not using --readOnly)", metavar="NumberChunks")
parser.add_option("-s", "--size", dest="size",
                  help="Chunk Max Size Defaults:64",default=64, metavar="SIZE")
parser.add_option("-S", "--scale", dest="scale",
                  help="Output Scaling (K,M,G) Default 'M'", default="M",metavar="SCALE")
parser.add_option("-u", "--username", dest="user",
                  help="Admin Auth User", metavar="USER")
parser.add_option("-p", "--password", dest="password",
                  help="Admin Auth Password", metavar="PASSWORD")
(options, args) = parser.parse_args()

#Parse and Validate Options
#Check Type,  Check Required Pairs

def mongo_connect(options,dbname,return_client=False):
  client      = MongoClient(options.host, int(options.port))
  dbconnector = client['admin']
  dbconnector.authenticate(options.user,options.password)
  if return_client == False:
    dbconnector = client[dbname]
  else:
    dbconnector = client
  return  dbconnector
def find_chunks(options):
    dbconnector       = mongo_connect(options,"config")
    worker_connector  = mongo_connect(options,"config",True)
    nameSpace         = options.database_name+"."+options.collection_name
    try:
      options.shardKey  = dbconnector.collections.find_one({"_id":nameSpace},{"key":1,"_id":0})['key']
    except:
      traceback.print_exc(file=sys.stdout)
      sys.exit(1)
    chunk_cursor  =  dbconnector.chunks.find({"ns": nameSpace})
    for chunk in chunk_cursor:
      check_chunk(options,chunk,worker_connector)

# NameSpace = {ns: "name",key: "shardKey",size: 1234,scale:"M"} 
def check_chunk(options,chunk,Connector):
  
  scales  = {
    "K" : 1024,
    "M" : 1024**2,
    "G" : 1024**3
  }
  dbconnector = Connector[chunk['ns'].split(".")[0]]
  command_odict=OrderedDict([("datasize",chunk['ns']),("keyPattern",options.shardKey),("min",chunk['min']),("max",chunk['max'])])
  ds          = dbconnector.command(command_odict)
  output_dict = { 
    "shard":  chunk['shard'],
    "size": round(float(ds['size']/scales[options.scale]), 2),    
    "chunk":  chunk['_id']
  }
  if (output_dict['size'] > float(options.size)):
    output="Shard: %(shard)s\t\tSize: %(size)s\t Chunk: %(chunk)s" % output_dict
    print(output)
  
if __name__ == "__main__":
  find_chunks(options)
