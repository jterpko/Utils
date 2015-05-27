#! /usr/bin/env python

from __future__ import print_function

import argparse
import collections
import sys


from fabric.colors import red
from pymongo import MongoClient, errors
from prettytable import PrettyTable


class ChunkHunter(object):
    def __init__(self, args):
        self.args = args
        self.host = args.host
        self.port = args.port
        self.database = args.database
        self.collection = args.collection
        self.user = args.user
        self.password = args.password
        self.noauth = args.noauth
        self.mode = args.check_mode
        self.autodrop = args.autodrop
        self.jumbos_found = None
        (self.output_database, self.output_collection) = args.output_ns.split(".")
        self.ns = '.'.join([self.database, self.collection])
        self.conn = self.connect_mongos()
        self.shard_key = self.get_shard_key()
        self.main()

    def connect_mongos(self):
        try:
            client = MongoClient(self.host, int(self.port), document_class=collections.OrderedDict)
        except Exception as e:
            sys.exit(red("Unabled to create connection to mongos! Due to: {}".format(e)))

        if self.noauth is not True:
            try:
                client.admin.authenticate(self.user, self.password)
            except Exception as e:
                sys.exit(red("Unable to auth to admin on {}:{} due to {}".format(self.host, self.port, e)))

        return client

    def is_sharded(self):
        return self.conn.config.collections.find({"_id": self.ns, 'key': {'$exists': True}}).count() > 0

    def get_shard_key(self):
        if self.is_sharded():
            collection_obj = self.conn.config.collections.find_one({"_id": self.ns})
            return collection_obj.get('key', None)

        sys.exit(red("Collection is not sharded: {}".format(self.ns)))

    def max_chunk_size(self):
        return self.conn.config.settings.find_one({"_id": "chunksize"}).value

    def populate_output_collection(self):
        current_count = 1
        chunks = self.get_chunks("config.chunks")
        errors = []

        print("Populating Chunks fron config database:")

        for chunk in chunks:
            print("\tPopulating chunk {} of {}".format(current_count, chunks.count()), end="\r")

            sys.stdout.flush()
            current_count += 1

            outputDoc = {
                "_id": chunk['_id'],
                "ns": chunk['ns'],
                "shard": chunk['shard'],
                "min": chunk['min'],
                "max": chunk['max'],
                "size": -1,
                "docs": -1,
                "processed": False,
                "jumbo": -1
            }

            if len(chunk['min']) > 2:
                sys.exit(red("Count mode is not for use on collections with compound shard keys that have more than two fields."))

            try:
                self.conn[self.output_database][self.output_collection].insert(outputDoc)
            except:
                errors.append("Unable to save chunk: {}".format(chunk['_id']))
                pass

        if len(errors) > 0:
                print("\tWe found {} errors in {} documents".format(len(errors), chunks.count()))

        print("")

    def get_chunks(self, namespace):
        try:
            if namespace is not None:
                (db, coll) = namespace.split('.')
                return self.conn[db][coll].find({"ns": "{}.{}".format(self.database, self.collection)}, timeout=False)
            else:
                return self.conn.config.chunks.find({"ns": "{}.{}".format(self.database, self.collection)}, timeout=False)
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
            "datasize", outputDoc['ns'],
            keyPattern=self.shard_key,
            min=outputDoc['min'],
            max=outputDoc['max']
        )

        outputDoc['size'] = (ds['size'] / 1024 / 1024)
        outputDoc['docs'] = ds['numObjects']

        return outputDoc

    def process_chunk_with_count(self, outputDoc):
        findDoc = {}
        projectDoc = {}
        avg_doc_size = self.conn[self.database].command("collstats", self.collection)['avgObjSize']
        field_count = len(outputDoc['min'])

        if field_count == 1:
            for key in outputDoc['min']:
                findDoc.update({key: {'$gte': outputDoc['min'][key], '$lt': outputDoc['max'][key]}})
                projectDoc.update({key: 1, "_id": 0})
        elif field_count == 2:
            i = 1
            for key in outputDoc['min']:
                if i == 1:
                    field1_key = key
                    field1_min_value = outputDoc['min'][key]
                    field1_max_value = outputDoc['max'][key]
                    i = i + 1
                elif i == 2:
                    field2_key = key
                    field2_min_value = outputDoc['min'][key]
                    field2_max_value = outputDoc['max'][key]
                    i = i + 1
            if (field1_min_value != field1_max_value):
                findDoc = {'$or': [
                    {field1_key: field1_min_value, field2_key: {'$gte': field2_min_value}},
                    {'$and': [{field1_key: {'$gt': field1_min_value}}, {field1_key: {'$lt': field1_max_value}}]},
                    {field1_key: field1_max_value, field2_key: {'$lt': field2_max_value}}
                ]}
            else:
                findDoc = {field1_key: field1_min_value, field2_key: {'$gte': field2_min_value, '$lt': field2_max_value}}
            projectDoc.update({field1_key: 1, field2_key: 1, "_id": 0})

        doc_count = self.conn[self.database][self.collection].find(findDoc, projectDoc).count()
        outputDoc['docs'] = doc_count
        outputDoc['size'] = (doc_count * avg_doc_size) / 1024 / 1024

        return outputDoc

    def save_document(self, outputDoc):
        if cmp(self.outputDoc_orig, outputDoc) == 0:
            self.conn[self.output_database][self.output_collection].update({'_id': outputDoc['_id']}, outputDoc)
        else:
            print("Chunk {} was unchanged not saving!".format(outputDoc['_id']))

    def generate_report_by_namespace(self):
        report = {}
        chunks = self.get_chunks("{}.{}".format(self.output_database, self.output_collection))

        for chunk in chunks:
            if chunk['ns'] not in report:
                report[chunk['ns']] = {'count': 1, 'jumbo_by_size': 0, 'jumbo_by_docs': 0}
            else:
                report[chunk['ns']]['count'] += 1

            report[chunk['ns']]['jumbo_by_docs'] += (chunk['docs'] > 250000)
            report[chunk['ns']]['jumbo_by_size'] += (chunk['size'] > 64)

        return report

    def print_report(self):
        outputTable = PrettyTable(["Namespace", "Count", "Jumbo by Doc", "Jumbo by Size"])
        outputTable.align['Namespace'] = "l"

        for key, value in self.generate_report_by_namespace().iteritems():
            outputTable.add_row([key, value['count'], value['jumbo_by_docs'], value['jumbo_by_size']])

            if value['jumbo_by_docs'] or value['jumbo_by_size']:
                self.jumbos_found = True

        print(outputTable)

    def findSplittableChunks(self):
        find_doc = {
            "$and": [
                {"ns": "{}.{}".format(self.database, self.collection)},
                {"$or": [{"size": {"$gt": self.args.size}}, {"docs": {"$gt": self.args.docs}}]}
            ]
        }
        return self.conn[self.output_database][self.output_collection].find(find_doc).count()

    def main(self):
        if not self.conn.is_mongos:
            sys.exit(red("This tool is not for use with non sharded clusters!"))
        else:
            if self.autodrop is not True and self.conn[self.output_database][self.output_collection].count() > 0:
                sys.exit(
                    red(
                        "The output collection of {}.{} already has data please select another location!" %
                        (self.output_database, self.output_collection)
                    )
                )
            if self.autodrop is True:
                try:
                    self.conn[self.output_database][self.output_collection].rename("%s_old" % self.output_collection, dropTarget=True)
                except errors.OperationFailure:
                    pass

            self.populate_output_collection()
            chunks = self.get_chunks("{}.{}".format(self.output_database, self.output_collection))
            chunk_count = self.conn[self.output_database][self.output_collection].count()

            print("Processing Chunks for size using {}:".format(self.mode))

            for chunk in chunks:
                outputDoc = self.process_chunk(chunk)
                process_count = self.conn[self.output_database][self.output_collection].find({'processed': True}).count()
                print("\tProcessing {} of {}".format(process_count, chunk_count), end="\r")

                sys.stdout.flush()
                self.save_document(outputDoc)

            print("\nSummary Report")

            self.print_report()

            if self.jumbos_found is not None:
                print("\nWe recommend checking the output as you may have splittable chunks to improve your balance")

        print("")  # Final newline for readability

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chunk Size estimation tool for sharded collections", prog="ChunkHunter")
    parser.add_argument("-H", "--host", help="Cluster Mongos Hostname", required=True)
    parser.add_argument("-P", "--port", help="Cluster Mongos Port", required=True)
    parser.add_argument("-u", "--user", help="Admin user of the cluster", required=False)
    parser.add_argument("-p", "--password", help="Admin password", required=False)
    parser.add_argument("-n", "--noauth", help="Disable Auth Attempts", action='store_true', required=False)
    parser.add_argument("-d", "--database", help="Database to examine", required=True)
    parser.add_argument("-c", "--collection", help="Collection to examine", required=True)

    parser.add_argument("-O", "--output-ns", help="Namespace to save results to", required=True)
    parser.add_argument("-m", "--check-mode", help="Checking method to use [count,datasize]", required=True)
    parser.add_argument("-A", "--autodrop", help="Remove output-ns if it exists", action='store_true', required=False)

    args = parser.parse_args()
    if (args.noauth is None and (args.login is None or args.instance_name is None)):
        sys.exit(red("Must set login AND name if using authentication"))

    ChunkHunter(args)
