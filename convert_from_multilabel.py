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
                        dest='field', type=lambda x: int(x)-1, required=True,
                        help='column that contains label names')
    parser.add_argument('-V', '--value-field', metavar='COLUMN',
                        dest='vfield', type=lambda x: int(x)-1, required=True,
                        help='column that contains label values')
    parser.add_argument('-i', '--id-field', metavar='COLUMN',
                        dest='ifield', type=lambda x: int(x)-1, required=True,
                        help='column that contains IDs')
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
    writer.writerow(options.labels + header)
    rows = {}
    for row in table:
        lname,lvalue,id = row[options.field], row[options.vfield], row[options.ifield]
        rows.setdefault(id, [dict(zip(options.labels, [None] * len(options.labels))), row])
        rows[id][0][lname] = lvalue
        rows[id][1] = row
    for _,(labels,row) in rows.items():
        writer.writerow([labels[lname] for lname in options.labels] + row)
