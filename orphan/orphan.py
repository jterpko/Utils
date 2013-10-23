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

class Orphan( object ):

    def __init__( self ):
        self.config_connection =  MongoClient('localhost', 50002)
        self.config_connection['admin'].authenticate('dba','dba')

    def getChunks(self):

        db = self.config_connection['config']
        all_chunks = db.chunks.find()

        return all_chunks

    def saveBadChunk(self, doc, chunk, hostname, port):

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
        connection['admin'].authenticate('dba','dba')

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

        print "chunk:%s checking %s with query:%s" % (chunkdata['_id'], port, query_doc)

        bad_documents = collection.find(query_doc)
        thecount = bad_documents.count()
        for bad_document in bad_documents:
            self.saveBadChunk(bad_document, chunkdata['_id'], hostname, port)

        print "found: %i" % thecount

        return thecount

    def getPrimary(self, hostname, port):
        connection = MongoReplicaSetClient(hostname, int(port))
        connection['admin'].authenticate('dba','dba')

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
        connection['admin'].authenticate('dba','dba')

        return connection.primary

    def checkForOrphans(self):

        orphan_chunk_count = 0
        for chunk in self.getChunks():

            print "\n\n-----"
            print "processing chunk %s" % chunk['_id']

            shards_to_visit = self.getOppositeShards(chunk['shard'])
            for shard in shards_to_visit:

                (host, port) = self.parseShardStr(shard)
                print "\nchecking host: %s" % host
                orphan_chunk_count += self.queryForChunk( host, port, chunk)

        return orphan_chunk_count

orphan = Orphan()
out = orphan.checkForOrphans()
print "total:%i" % out



