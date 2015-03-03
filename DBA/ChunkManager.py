#! /usr/bin/env python

from __future__ import print_function

import argparse
import sys


from fabric.colors import red

from ChunkSplitter import ChunkSplitter
from ChunkHunter import ChunkHunter, findSplittableChunks

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chunk Size estimation tool for sharded collections", prog="ChunkHunter")
    #Connection Arguments
    parser.add_argument("-H", "--host", help="Cluster Mongos Hostname", required=True)
    parser.add_argument("-P", "--port", help="Cluster Mongos Port", required=True)
    parser.add_argument("-u", "--user", help="Admin user of the cluster", required=False)
    parser.add_argument("-p", "--password", help="Admin password", required=False)
    parser.add_argument("-n", "--noauth", help="Disable Auth Attempts", action='store_true', required=False)
    #Name Space Arguments
    parser.add_argument("-d", "--database", help="Database to examine", required=True)
    parser.add_argument("-c", "--collection", help="Collection to examine", required=True)
    parser.add_argument("-T", "--temp-namespace", help="Namespace to get/save results", required=True)
    #Removed  input and output in favor or "tmp"
    #parser.add_argument("-I", "--input-namespace", help="Namespace to get saved results from", required=True)
    #parser.add_argument("-O", "--output-ns", help="Namespace to save results to", required=True)

    #Hunter Arguments
    parser.add_argument("-m", "--check-mode", help="Checking method to use [count,datasize]", required=True)
    #Splitter Arguments
    parser.add_argument("-D", "--docs", help="Min # of documents for split. Can't use with the size option", type=int, required=False, default=200000)
    parser.add_argument("-s", "--size", help="Min size of documents for split. Can't use with the docs option", type=int, required=False, default=64)

    #Removed split middle as recursive will make more sense in this tool
    parser.add_argument("--debug", help="Enable Debug", action='store_true', required=False)

    args = parser.parse_args()
    #Lets do some post processing to make the arguments better ;)

    if args.noauth is not None:
        if args.user is None or args.password is None:
            sys.exit(red("You must have a valid user/pass combo or noauth set."))

    args.input_namespace = args.temp_namespace
    args.output_ns = args.temp_namespace
    args.autodrop = True

    if args.debug is True:
        print(args)

    #Lets do some work
    ChunkHunter(args)
    current_splittable_count = findSplittableChunks(args)
    while current_splittable_count > 0:
        ChunkSplitter(args)
        ChunkHunter(args)
        current_splittable_count = findSplittableChunks(args)
