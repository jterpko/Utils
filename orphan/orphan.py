#!/usr/bin/python
#
# create a report of orphan documents in a cluster
#
# creates a database called _orphandocs and populates with documents that are orphaned
# from the config servers point of view
#
#

import sys
import time
import pymongo
import bson
import datetime
import json
from pymongo import MongoReplicaSetClient
from pymongo import MongoClient
from optparse import OptionParser

class Orphan( object ):

    def __init__( self ):
        self.config_connection =  MongoClient(options.host, options.port)
        self.config_connection['admin'].authenticate(options.username, options.password)

    def writeLine(self,message,newLine=False):
        if newLine == True:
            print("")
        sys.stdout.write(message)
        sys.stdout.flush()
        sys.stdout.write('\r')
        sys.stdout.flush()

    def getBalancerState(self):
        try:
            balancer_state = self.config_connection['config']['settings'].find_one({"_id":"balancer"},{"_id":0,"stopped":1})
        except Exception, e:
            print e

        if balancer_state:
            return balancer_state['stopped']
        else:
            print "unable to determine state of Balancer, check config.settings"
            sys.exit(0)

    def setBalancer(self, state):
        if isinstance(state,bool):
            state = no state
        try:
            self.config_connection['config']['settings'].update({"_id":"balancer"}, {"$set" : { "stopped": state}}, True, False)
        except Exception, e:
            print e
        return True

    def checkBalancer(self):
        """ returns True if balancer is stopped """
        try:
            lock_count = self.config_connection['config']['locks'].find_one({ "_id": "balancer" })
        except Exception, e:
            print e

        if 'state' in lock_count and lock_count['state'] > 0:
            return False
        else:
            return True

    def getChunksCount(self):
        """ return chunk count """

        db = self.config_connection['config']
        if not options.namespace:
            return db.chunks.count()
        else:
            return db.chunks.find({"ns":options.namespace}).count()

    def getChunks(self):
        """ return chunk list """

        chunks = []
        db = self.config_connection['config']
        if not options.namespace:
            all_chunks = db.chunks.find()
        else:
            all_chunks = db.chunks.find({"ns":options.namespace})

        for chunk in all_chunks:
            chunks.append(chunk)

        return chunks

    def saveBadDocument(self, doc, chunk, hostname, port, db, collection):
        """ saves data to host:port in a collection for review """

        bad_chunk_db = self.config_connection['_orphandocs']
        bad_chunk_collection = bad_chunk_db['orphandocs']

        master_document = {"host":hostname,
                            "port":port,
                            "doc":doc,
                            "chunk":chunk,
                            "database":db,
                            "collection":collection,
                            "thedate":datetime.datetime.utcnow()}

        try:
            if options.verbose: print ("logging bad document")
            bad_chunk_collection.save(master_document)
        except Exception, e:
            print e

        return True

    def isHashedKey(self, ns):
        collection_data = self.config_connection['config']['collections'].find_one({ "_id": ns })
        if 'key' in collection_data:
            if collection_data['key'][collection_data['key'].keys()[0]] == "hashed":
                return True
        return False

    def parseNS(self, ns):
        """ return split ns string """
        split_namespace = ""
        if isinstance(ns, unicode):
            split_namespace = ns.split('.')
        return split_namespace

    def queryForChunk(self, hostname, port, chunkdata):
        """ find data on a specific host:port based on chunkdata range """

        thecount = -1
        query_doc = {}

        connection = MongoClient(hostname, int(port))
        connection['admin'].authenticate(options.username,options.password)

        (d, c) = self.parseNS(chunkdata['ns'])

        database = connection[d]
        collection = database[c]

        shard_key = chunkdata['min'].keys()[0]
        key_list = {shard_key:1}

        # detect if this is a center, top or bottom style chunk using the type of the key
        if isinstance(chunkdata['max'][shard_key], bson.max_key.MaxKey):
            query_doc = { shard_key:{"$gte": chunkdata['min'][shard_key] } }

        elif isinstance(chunkdata['min'][shard_key], bson.min_key.MinKey):
            query_doc = { shard_key:{ "$lt": chunkdata['max'][shard_key] } }

        else:
            query_doc = { shard_key:{ "$gte": chunkdata['min'][shard_key], "$lt": chunkdata['max'][shard_key] }  }

        if options.verbose: print ("chunk:%s checking %s with query:%s") % (chunkdata['_id'], port, query_doc)

        if not options.fastmode:
            bad_documents = collection.find(query_doc, key_list)
            thecount = bad_documents.count()
            for bad_document in bad_documents:
                self.saveBadDocument( bad_document, chunkdata['_id'], hostname, port, d, c )
        else:
            thecount = collection.find(query_doc).count()
            if thecount > 0:
                self.saveBadDocument( {"count":thecount}, chunkdata['_id'], hostname, port, d, c )

        if options.verbose: print ("found: %i") % thecount

        return thecount

    def getPrimary(self, hostname, port):
        """ Return the primary for a cluster """
        connection = MongoReplicaSetClient(hostname, int(port))
        connection['admin'].authenticate(options.username,options.password)

        return connection.primary

    def getOppositeShards(self, shardName):
        """ returns all shards that this chunks is *NOT* part of """
        shards = []
        db = self.config_connection['config']
        all_shards = db.shards.find()

        for i in all_shards:
            if (shardName != i['_id']):
                shards.append(i['host'])

        return shards

    def parseShardStr(self, shardStr):
        """ Takes string and parses to array of dics with host and port keys """

        seedstr = shardStr.split('/')[1].split(',')[0]
        replicaset = shardStr.split('/')[0]

        connection = MongoReplicaSetClient(seedstr, replicaSet = replicaset)
        connection['admin'].authenticate(options.username,options.password)

        return connection.primary

    def checkForOrphans(self):

        chunks = []
        total_chunks = self.getChunksCount()
        orphan_chunk_count = 0
        current_chunk_count = 0

        for chunk in self.getChunks():
            current_chunk_count += 1
            self.writeLine("Proccessing chunk %s of %s " % (current_chunk_count,total_chunks))
            if 'ns' in chunk and not self.isHashedKey(chunk['ns']):
                shards_to_visit = self.getOppositeShards(chunk['shard'])
                for shard in shards_to_visit:
                    (host, port) = self.parseShardStr(shard)
                    if options.verbose: print "\nchecking host: %s" % host
                    orphan_chunk_count += self.queryForChunk( host, port, chunk )

            else:

                if options.verbose: print "chunk is hashed! skipping %s" % chunk
                pass

        return orphan_chunk_count

if __name__ == "__main__":

    parser = OptionParser()
    parser.set_defaults(host="localhost",port=27017)
    parser.add_option("--host", dest="host", help="hostname to connect to")
    parser.add_option("--port", dest="port", type=int, help="port to connect to")
    parser.add_option("--ns", dest="namespace", default=False, help="limit results this NS")
    parser.add_option("--username", dest="username", help="username")
    parser.add_option("--password", dest="password", help="password")
    parser.add_option("--fastmode", dest="fastmode", action="store_true", default=True, help="quick pass mode or detailed logging of each orphan")
    parser.add_option("--verbose", dest="verbose", action="store_true", default=False, help="have verbose output about what is being checked")
    
    (options, args) = parser.parse_args()

    orphan_output = {}

    orphan = Orphan()

    old_state = orphan.getBalancerState()
    orphan.setBalancer(False)

    if orphan.checkBalancer():
        orphan_output['orphan_document_count'] = orphan.checkForOrphans()
    elif time.sleep(30) and orphan.checkBalancer():
        orphan_output['orphan_document_count'] = orphan.checkForOrphans()
    else:
        print "balancer is running, exiting.."
        sys.exit(0)

    orphan.setBalancer(old_state)

    print json.dumps(orphan_output, indent=4)
