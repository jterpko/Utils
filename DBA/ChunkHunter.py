#! /usr/bin/env python

import argparse
import sys


from fabric.colors import red, white
from pymongo import MongoClient
from pymongo import ReadPreference


class ChunkFinder(object):
    def __init__(self, args):
        self.database = args.database
        self.collection = args.collection
        self.user = args.user
        self.password = args.password
        self.authdb = args.authdb
        self.mode = args.check_mode
        self.conn = None
        (self.output_database, self.output_collection) = args.output_ns.split(".")
        self.ns = "%s.%s" % (self.database, self.collection)
        self.conn = self.connect_mongos()
        self.shard_key = self.get_shard_key()
        self.main()

    def is_sharded(self):
        return self.conn.config.collections.find({"_id": self.ns, 'key': {'$exists': True}}).count() > 0

    def get_shard_key(self):
        if self.is_sharded():
                collection_obj = self.conn.config.collections.find_one({"_id": self.ns})
                return collection_obj.get('key', None)
        else:
                sys.exit(red("Collection is not sharded: {}".format(self.ns)))

    def max_chunk_size(self):
        return self.conn.config.settings.find_one({"_id": "chunksize"}).value

    def populate_output_collection(self):
        for chunk in self.get_chunks("config.chunks"):
            print("Populating chunk: %s" % chunk['_id'])
            outputDoc = {
                "_id": chunk['_id'],
                "ns": chunk['ns'],
                "shard": chunk['shard'],
                "min": chunk['min'],
                "max": chunk['max'],
                "size": -1,
                "docs": -1,
                "processed": False,
                "jumbo": -1,
                "runDate": self.run_date
            }
            try:
                self.conn[self.output_database][self.output_collection].insert(outputDoc)
            except:
                print "Unable to save chunk: %s" % chunk['_id']
                pass

    def get_chunks(self, namespace):
        try:
            if namespace is not None:
                (db, coll) = namespace.split('.')
                return self.conn[db][coll].find({"ns": "%s.%s" % (self.database, self.collection)})
            else:
                return self.conn.config.chunks.find({"ns": "%s.%s" % (self.database, self.collection)})
        except:
            return {}

    def process_chunk(self, chunk):
        outputDoc = chunk
        self.outputDoc_orig = outputDoc
        if self.mode == "datasize":
            outputDoc = self.process_chunk_with_datasize(outputDoc)
        elif self.mode == "count":
            outputDoc = self.process_chunk_with_count(outputDoc)
        else:
            return outputDoc
        outputDoc['processed'] = True
        outputDoc['jumbo'] = True if outputDoc['size'] > self.max_chunk_size else False
        return outputDoc

    def process_chunk_with_datasize(self, outputDoc):
        ds = self.conn[self.database].command(
            "datasize", outputDoc.ns,
            keyPattern=self.shard_key,
            min=outputDoc['min'],
            max=outputDoc['max']
        )
        outputDoc['size'] = (ds.size / 1024 / 1024)
        outputDoc['docs'] = ds.numObjects
        return outputDoc

    def process_chunk_with_count(self, outputDoc):
        findDoc = {}
        projectDoc = {}
        avg_doc_size = self.conn[self.database].command("collstats", self.collection)['avgObjSize']
        for key in outputDoc['min']:
            findDoc.update({key: {'$gte': outputDoc['min'][key], '$lt': outputDoc['max'][key]}})
            projectDoc.update({key: 1})
        doc_count = self.conn[self.database][self.collection].find(findDoc, projectDoc).count()
        outputDoc['docs'] = doc_count
        outputDoc['size'] = (doc_count * avg_doc_size) / 1024 / 1024
        return outputDoc

    def save_document(self, outputDoc):
        if cmp(self.outputDoc_orig, outputDoc) == 0:
            self.conn[self.output_database][self.output_collection].update({'_id': outputDoc['_id']}, outputDoc)
            print(self.conn[self.output_database].command({"getLastError": 1}))
        else:
            print("Chunk %s was unchanged not saving!" % outputDoc['_id'])

    def main(self):
        if self.inst.type == "mongodb_replica_set":
            sys.exit(red("Unable to process replica set instances!"))
        else:
            if self.is_sharded():
                self.populate_output_collection()
                for chunk in self.get_chunks("{}.{}".format(self.output_database, self.output_collection)):
                    outputDoc = self.process_chunk(chunk)
                    self.save_document(outputDoc)
            else:
                sys.exit(white("{} is not sharded".format(red(self.database+"."+self.collection))))
        print  # Final newline for readability

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Report on disk usage of all vz's for an instance", prog="chunk_size_estimator")
    parser.add_argument("-o", "--objid", help="ObjectID of the Instance", required=False)
    parser.add_argument("-l", "--login", help="User login of the Instance", required=False)
    parser.add_argument("-i", "--instance-name", help="Name of the Instance", required=False)

    parser.add_argument("-d", "--database", help="Database to examine", required=True)
    parser.add_argument("-c", "--collection", help="Collection to examine", required=True)
    parser.add_argument("-O", "--output-ns", help="Namespace to save results to", required=True)
    parser.add_argument("-m", "--check-mode", help="Checking method to use [count,datasize]", required=True)

    args = parser.parse_args()
    if (args.objid is None and (args.login is None or args.instance_name is None)):
        sys.exit(red("Must set objid or  login AND name"))

    ChunkFinder(args)
