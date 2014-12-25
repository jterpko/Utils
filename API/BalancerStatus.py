import argparse
import httplib
import json

from datetime import datetime


def get_collection_data(conn, auth_params, headers, database, collection):
    method = "/db/{}/collection/{}/stats/get".format(database, collection)
    conn.request("POST", method, auth_params, headers)
    response = conn.getresponse()

    resp_json = json.loads(response.read())
    chunks = resp_json.get('data', {}).get('chunks', [])

    return chunks


def find_chunk_count_per_shard(chunks):
    chunk_map = {}
    for chunk in chunks:
        shard = chunk.get('shard', 'XXUnknownXX')
        if shard in chunk_map:
            chunk_map[shard] += 1
        else:
            chunk_map[shard] = 1

    return chunk_map


def main(args):
    # Build HTTP Conn
    host = "api.objectrocket.com:80"
    conn = httplib.HTTPConnection(host)

    # Assign auth params
    auth_params = "api_key={}".format(args.apikey)

    # Define the headers for the HTTP POST.
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}

    right_now = datetime.utcnow()

    chunks = get_collection_data(conn, auth_params, headers, args.database, args.collection)

    chunk_map = find_chunk_count_per_shard(chunks)

    print "As of {}Z (UTC) here is the current chunk distribution:".format(right_now)
    print json.dumps(chunk_map, sort_keys=True, indent=4)

    # Close the HTTP connection.
    conn.close()

if __name__ == "__main__":
    # Build argument parser
    parser = argparse.ArgumentParser(description='Gather current chunk counts for shards.')
    parser.add_argument('-a',
                        '--apikey',
                        help="API Key for instance",
                        type=str,
                        required=True)
    parser.add_argument('-d',
                        '--database',
                        help="Name of database to poll",
                        type=str,
                        required=True)
    parser.add_argument('-c',
                        '--collection',
                        help="Name of collection to poll",
                        type=str,
                        required=True)

    args = parser.parse_args()
    main(args)
