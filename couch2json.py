#!/usr/bin/env python
import argparse
import json


def main(args):
    print args

    with open(args.inputfile, 'r') as input_file:
        if args.outputfile:
            output_file_name = args.outputfile
        else:
            output_file_name = 'couch_converted.json'

        print "Writing converted file to {}".format(output_file_name)

        with open(output_file_name, 'w') as output_file:
            lines_read = 0

            for line in input_file:
                if line.startswith('"total_rows"') or line.startswith("]}"):
                    continue
                try:
                    stripped_line = line.replace(',\r\n', '')
                    json_object = json.loads(stripped_line)
                    doc = json_object['doc']
                    doc.pop('_id')
                    doc.pop('_rev')
                    output_file.write(json.dumps(doc) + "\r\n")
                    lines_read += 1
                except Exception as ex:
                    print "Failed to convert line : {}".format(ex)

                if lines_read % 1000 == 0:
                    print "{} documents processed".format(lines_read)

        print "{} documents processed".format(lines_read)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converts CouchDB exports to regular JSON files.")
    parser.add_argument('inputfile', metavar='INPUT_FILE', type=str, help='The CouchDB file to import.')
    parser.add_argument('-o', '--outputfile', help='The name of the output file.')

    args = parser.parse_args()
    main(args)
