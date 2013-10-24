#!/usr/bin/python
#
# create a report of orphan documents in a cluster
#
# creates a database called _orphandocs and populates with documents that are orphaned
# from the config servers point of view
#
#

import pymongo
import bson
import datetime
from pymongo import MongoReplicaSetClient
from pymongo import MongoClient
from optparse import OptionParser

class Orphan( object ):

    def __init__( self ):
        self.config_connection =  MongoClient(options.host, options.port)
        self.config_connection['admin'].authenticate(options.username, options.password)

    def getBalancerState(self):
        try:
            balancer_state = self.config_connection['config']['settings'].find_one({"_id":"balancer"},{"_id":0,"stopped":1})
        except Exception, e:
            print e
        return balancer_state['stopped']

    def setBalancer(self, state):
        try:
            self.config_connection['config']['settings'].update({"_id":"balancer"}, {"$set" : { "stopped": state}}, True, False)
        except Exception, e:
            print e
        return True

    def checkBalancer(self):
        try:
            lock_count = self.config_connection['config']['locks'].find({ _id: "balancer" }).count()
        except Exception, e:
            print e

        if lock_count != 0:
            return False
        else:
            return True

    def getChunks(self):

        db = self.config_connection['config']
        all_chunks = db.chunks.find()

        return all_chunks

    def saveBadDocument(self, doc, chunk, hostname, port):

        bad_chunk_db = self.config_connection['_orphandocs']
        bad_chunk_collection = bad_chunk_db['orphandocs']

        master_document = {"host":hostname, "port":port, "doc":doc, "chunk":chunk, "thedate":datetime.datetime.utcnow()}

        try:
            bad_chunk_collection.save(master_document)
        except Exception, e:
            print e

        return True

    def queryForChunk(self, hostname, port, chunkdata):

        thecount = -1
        query_doc = {}

        connection = MongoClient(hostname, int(port))
        connection['admin'].authenticate(options.username,options.password)

        (d, c) = chunkdata['ns'].split('.')

        database = connection[d]
        collection = database[c]

        shard_key = chunkdata['min'].keys()[0]

        if isinstance(chunkdata['max'][shard_key], bson.max_key.MaxKey):
            query_doc = { shard_key:{"$gte": chunkdata['min'][shard_key] } }

        elif isinstance(chunkdata['min'][shard_key], bson.min_key.MinKey):
            query_doc = { shard_key:{ "$lte": chunkdata['max'][shard_key] } }

        else:
            query_doc = { shard_key:{ "$gte": chunkdata['min'][shard_key], "$lte": chunkdata['max'][shard_key] } }

        if options.verbose: print ("chunk:%s checking %s with query:%s") % (chunkdata['_id'], port, query_doc)

        bad_documents = collection.find(query_doc)
        thecount = bad_documents.count()
        for bad_document in bad_documents:
            self.saveBadDocument(bad_document, chunkdata['_id'], hostname, port)

        if options.verbose: print ("found: %i") % thecount

        return thecount

    def getPrimary(self, hostname, port):
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
        """ I hate string processing, takes string and parses to array of dics with host and port keys """

        seedstr = shardStr.split('/')[1].split(',')[0]
        replicaset = shardStr.split('/')[0]

        connection = MongoReplicaSetClient(seedstr, replicaSet = replicaset)
        connection['admin'].authenticate(options.username,options.password)

        return connection.primary

    def checkForOrphans(self):

        orphan_chunk_count = 0
        for chunk in self.getChunks():

            if options.verbose: print ("\n\nprocessing chunk %s") % chunk['_id']

            shards_to_visit = self.getOppositeShards(chunk['shard'])
            for shard in shards_to_visit:

                (host, port) = self.parseShardStr(shard)
                if options.verbose: print "\nchecking host: %s" % host
                orphan_chunk_count += self.queryForChunk( host, port, chunk)

        return orphan_chunk_count

if __name__ == "__main__":

    parser = OptionParser()
    parser.set_defaults(host="localhost",port=27017)
    parser.add_option("--host", dest="host", help="hostname to connect to")
    parser.add_option("--port", dest="port", type=int, help="port to connect to")
    parser.add_option("--username", dest="username", help="username")
    parser.add_option("--password", dest="password", help="password")
    parser.add_option("--verbose", dest="verbose", help="have verbose output about what is being checked")
    (options, args) = parser.parse_args()

    orphan = Orphan()

    old_state = orphan.getBalancerState()
    orphan.setBalancer(False)
    out = orphan.checkForOrphans()
    orphan.setBalancer(old_state)



