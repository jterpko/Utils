#!/usr/bin/python
#
# create a report of orphan chunks in a cluster
#

import pymongo
import bson
from pymongo import MongoClient

class Orphan( object ):

    def __init__( self ):
        self.config_connection =  MongoClient('localhost', 50002)
        self.config_connection['admin'].authenticate('dba','dba')

    def getChunks(self):

        db = self.config_connection['config']
        all_chunks = db.chunks.find()

        return all_chunks

    def queryForChunk(self, hostname, port, chunkdata):

        thecount = 0

        connection = MongoClient(hostname, int(port))
        connection['admin'].authenticate('dba','dba')

        (d, c) = chunkdata['ns'].split('.')

        database = connection['d']
        collection = database['c']

        #print "chunk:%s checking %s for max:%s min:%s\n" % (chunkdata['_id'], port, chunkdata['max'], chunkdata['min'])

        # this annoys me, is there a better way?

        if isinstance(chunkdata['max']['_id'], bson.max_key.MaxKey):
            query_doc = {"$gte": chunkdata['min'] }
        elif isinstance(chunkdata['min']['_id'], bson.min_key.MinKey):
            query_doc = { "$lte": chunkdata['max']}
        else:
            query_doc = { "$gte": chunkdata['min'], "$lte": chunkdata['max'] }

        print "   query:"+str(query_doc)

        try:
            thecount = collection.find(query_doc).count()
        except:
            print "      hrm, we too low"

        print "   found: %i" % thecount

        return thecount

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

        out = []
        hosts = shardStr.split('/')[1].split(',')

        for i in hosts:
            (host, port) = i.split(':')
            doc = {"host":host, "port":port}
            out.append(doc)

        return out

    def checkForOrphans(self):

        orphan_chunk_count = 0
        for chunk in self.getChunks():

            print "processing chunk %s" % chunk['_id']

            shards_to_visit = self.getOppositeShards(chunk['shard'])
            for shard in shards_to_visit:
                hosts = self.parseShardStr(shard)
                for host in hosts:
                    print "   checking host: %s" % host
                    orphan_chunk_count += self.queryForChunk( host['host'], host['port'], chunk)

        return orphan_chunk_count

orphan = Orphan()
out = orphan.checkForOrphans()
print "total:%i" % out



