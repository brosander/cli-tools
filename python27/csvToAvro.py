#!/usr/bin/python
import csv
import argparse
from avro.schema import make_avsc_object
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='''
    This script is a command line tool to read a csv file and output it as an Avro file.
  ''', formatter_class = argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("-i", "--input", default="/dev/stdin", help="Csv input file")
  parser.add_argument("-o", "--output", default="/dev/stdout", help="Csv output file")
  parser.add_argument("-s", "--namespace", default="csv.to.avro", help="Avro namespace")
  parser.add_argument("-n", "--name", default="Record", help="Avro name")
  args = parser.parse_args()
  with open(args.input, 'rb') as csvfile:
    reader = csv.DictReader(csvfile)
    first = True
    try:
      for row in reader:
        if first:
          fields = []
          for field in row:
            fields.append({'name': field, 'type': 'string'})
          schema = make_avsc_object({'namespace': args.namespace, 'type': 'record', 'name': args.name, 'fields': fields})
          writer = DataFileWriter(open(args.output, "w"), DatumWriter(), schema)
          first = False
        writer.append(row)
    finally:
      writer.close()
