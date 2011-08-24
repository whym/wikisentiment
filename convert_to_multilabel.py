#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import argparse
import sys
import ast

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-f', '--field', metavar='COLUMN',
                        dest='field', type=int, required=True,
                        help='column that contains labels')
    parser.add_argument('-D', '--delimiter', metavar='CHARACTER',
                        dest='delimiter', type=lambda x: ast.literal_eval('"'+x+'"'), default=',',
                        help='')
    parser.add_argument('-l', '--labels', metavar='COLUMNS',
                        dest='labels', type=lambda x: s.split(','), default=['Praise/thanks','Criticism/insult','Other'],
                        help='label values')

    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    parser.add_argument('input', nargs='?', default=sys.stdin)
    options = parser.parse_args()

    table = list(csv.reader(options.input, delimiter=options.delimiter))
    writer = csv.writer(options.output, delimiter=options.delimiter)

    header,table = table[0],table[1:]
    writer.writerow(header[0:options.field-1] + options.labels + header[options.field+1:])
    for row in table:
        labels = [1 if x == row[options.field-1] else 0 for x in options.labels]
        if labels == [0] * len(options.labels):
            labels = [''] * len(options.labels)
        writer.writerow(row[0:options.field-1] + labels + row[options.field+1:])
