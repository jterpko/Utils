#! /usr/bin/env python

import argparse
import sys
import time
import decimal
from decimal import *
import math

from bson.min_key import MinKey
from bson.max_key import MaxKey
from fabric.colors import red
from pymongo import MongoClient


class ChunkSplitter(object):
    def __init__(self, args):
        self.host = args.host
        self.port = args.port
        self.database = args.database
        self.collection = args.collection
        self.user = args.user
        self.password = args.password
        self.noauth = args.noauth

        self.use_middle = args.split_using_middle
        self.docs = args.docs
        self.size = args.size

        self.conn = None
        (self.input_database, self.input_collection) = args.input_namespace.split(".")
        self.conn = self.connect_mongos()
        self.ns = "%s.%s" % (self.database, self.collection)
        self.main()

    def connect_mongos(self):
        try:
            client = MongoClient(self.host, int(self.port))
        except Exception as e:
            sys.exit(red("Unabled to create connection to mongos! Due to: {}".format(e)))

        if self.noauth is not True:
            try:
                client.admin.authenticate(self.user, self.password)
            except Exception as e:
                sys.exit(red("Unable to auth to admin on {}:{} due to {}".format(self.host, self.port, e)))

        return client

    def is_sharded(self):
        if self.conn.config.collections.find({"_id": self.ns, 'key': {'$exists': True}}).count() > 0:
            return True
        else:
            return False

    def is_hashed(self):
        if self.is_sharded():
                collection_obj = self.conn.config.collections.find_one({"_id": self.ns})
                try:
                    for keypair in collection_obj['key'].iteritems():
                        if keypair[1] == "hashed":
                            return True
                except:
                        pass
                return False
        else:
                sys.exit(red("Collection is not sharded: {}".format(self.ns)))

    def max_chunk_size(self):
        chunk_size = self.conn.config.settings.find_one({"_id": "chunksize"})
        return chunk_size['value']

    def get_chunks(self, namespace):
        try:
            if namespace is not None:
                (db, coll) = namespace.split('.')
                findObj = {
                    'ns': "%s.%s" % (self.database, self.collection),
                    'docs': {'$gt': self.docs},
                    'size': {'$gt': self.size}
                }
                return self.conn[db][coll].find(findObj)
        except:
            return {}

    def process_chunk(self, chunk):
        outputDoc = chunk
        self.outputDoc_orig = outputDoc
        if (self.use_middle is True):
            outputDoc['split'] = self.splitChunkMiddle(outputDoc)
        else:
            outputDoc['split'] = self.splitChunk(outputDoc)
        return outputDoc

    def splitChunk(self, chunk):
        result = None
        if 'size' in chunk:
            for key, value in chunk['min'].iteritems():
                if value == MinKey:
                    print("We skipped {} due to having a minKey on {}".format(chunk['_id'], key))
                    return False
            for key, value in chunk['max'].iteritems():
                if value == MaxKey:
                    print("We skipped {} due to having a maxKey on {}".format(chunk['_id'], key))
                    return False
            try:
                if self.is_hashed():
                    result = self.conn.admin.command("split", chunk['ns'], bounds=[chunk['min'], chunk['max']])
                else:
                    result = self.conn.admin.command("split", chunk['ns'], find=chunk['min'])
            except Exception as e:
                print("Failed to  run split due to {}".format(e))
                pass
            self.conn.admin.command("flushRouterConfig")
            return True if result is not None and (result['ok'] == 1 or result['ok'] == 1.0) else None
        else:
            return None

    def splitChunkMiddle(self, chunk):
        if 'size' in chunk:
            for key, value in chunk['min'].iteritems():
                if value == MinKey():
                    print("We skipped {} due to having a minKey on {}".format(chunk['_id'], key))
                    return False
            for key, value in chunk['max'].iteritems():
                if value == MaxKey():
                    print("We skipped {} due to having a maxKey on {}".format(chunk['_id'], key))
                    return False
            key_name = chunk['min'].iteritems().next()[0]
            min_value = Decimal(chunk['min'].iteritems().next()[1])
            max_value = Decimal(chunk['max'].iteritems().next()[1])
            desired_chunks = Decimal(math.ceil(chunk['size'] / self.max_chunk_size()))
            step_size = ((min_value - max_value) / desired_chunks).quantize(Decimal(0e-50), rounding=decimal.ROUND_DOWN)
            if (step_size == 0 or step_size == -0):
                print("Step was %s" % step_size)
                return False
            if(Decimal(min_value).is_signed() or Decimal(max_value).is_signed()):
                split_points = range(max_value, min_value, step_size)
            else:
                split_points = range(min_value, max_value, step_size)
            errors = 0
            for split_point in split_points:
                try:
                    self.conn.admin.command("split", chunk['ns'], middle={key_name: long(split_point)})
                except Exception as e:
                    print(e)
                    print("Failed to  run split due to {}".format(e))
                    pass
                    errors += 1
            return True if errors != len(split_points) else False
        else:
            return False

    def save_document(self, outputDoc):
        if cmp(self.outputDoc_orig, outputDoc) == 0:
            self.conn[self.input_database][self.input_collection].update({'_id': outputDoc['_id']}, outputDoc)
            #print(self.conn[self.input_database].command({"getLastError": 1}))
        else:
            print("Chunk %s was unchanged not saving!" % outputDoc['_id'])

    def main(self):
        counter = 0
        total_count = len(list(self.get_chunks("{}.{}".format(self.input_database, self.input_collection))))
        for chunk in self.get_chunks("{}.{}".format(self.input_database, self.input_collection)):
            outputDoc = self.process_chunk(chunk)
            counter += 1
            print("%s/%s" % (counter, total_count))
            time.sleep(0.05)
            self.save_document(outputDoc)
        print  # Final newline for readability

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Report on disk usage of all vz's for an instance", prog="chunk_size_estimator")
    parser.add_argument("-H", "--host", help="ObjectID of the Instance", required=True)
    parser.add_argument("-P", "--port", help="ObjectID of the Instance", required=True)
    parser.add_argument("-u", "--user", help="ObjectID of the Instance", required=False)
    parser.add_argument("-p", "--password", help="ObjectID of the Instance", required=False)
    parser.add_argument("-D", "--docs", help="Min # of documents for split. Can't use with the size option", type=int, required=False, default=200000)
    parser.add_argument("-s", "--size", help="Min size of documents for split. Can't use with the docs option", type=int, required=False, default=64)
    parser.add_argument("-d", "--database", help="Database to examine", required=True)
    parser.add_argument("-c", "--collection", help="Collection to examine", required=True)
    parser.add_argument("-I", "--input-namespace", help="Namespace to get saved results from", required=True)
    parser.add_argument("-n", "--noauth", help="ObjectID of the Instance", required=False)
    parser.add_argument(
        "-M",
        "--split-using-middle",
        action="store_true",
        default=False,
        help="For split to use middle and make bucket assumptions using the size argument and the chunk size.",
        required=False
    )
    args = parser.parse_args()
    print(args)
    ChunkSplitter(args)
