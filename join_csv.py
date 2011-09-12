#! /usr/bin/env python
# -*- coding: utf-8 -*-

# assuming 1st csv contains no duplicate based on the ID column

import csv
import argparse
import sys
import ast
from myutils import int_if

def join(table1, table2, j1, j2):
    table1 = sorted(table1, key=j1)
    table2 = sorted(table2, key=j2)

    len1 = len(table1[0])
    len2 = len(table2[0])

    while len(table1) > 0 and len(table2) > 0:
        #print j1(table1[0]), j2(table2[0])
        if j1(table1[0]) == j2(table2[0]):
            yield table1[0] + table2[0]
            table2 = table2[1:]
            if len(table2) == 0:
                break
        while j1(table1[0]) > j2(table2[0]):
            yield ['']*len1 + table2[0]
            table2 = table2[1:]
            if len(table2) == 0:
                break
        if len(table2) == 0:
            break
        if len(table1) > 0 and j1(table1[0]) < j2(table2[0]):
            yield table1[0] + ['']*len2
            table1 = table1[1:]
    for cols in table2:
        yield ['']*len1 + cols
    for cols in table1:
        yield cols + ['']*len2

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-j1', '--join-on-1', metavar='N',
                        dest='j1', type=int, required=True,
                        help='column of the first table that is used to join the tables')
    parser.add_argument('-j2', '--join-on-2', metavar='N',
                        dest='j2', type=int, required=True,
                        help='column of the second table that is used to join the tables')
    parser.add_argument('-D', '--delimiter', metavar='CHARACTER',
                        dest='delimiter', type=lambda x: ast.literal_eval('"'+x+'"'), default=',',
                        help='')
    parser.add_argument('-H', '--header',
                        dest='header', action='store_true', default=False,
                        help='')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    parser.add_argument('-I', '--non-int-key',
                        dest='nonint', action='store_true', default=False,
                        help='')
    parser.add_argument('inputs', nargs='+')
    options = parser.parse_args()

    tables = []
    headers = []
    for fname in options.inputs[0:2]:
        table = [x for x in csv.reader(open(fname,'rU'), delimiter=options.delimiter) if x != []]
        if options.header:
            header,table = table[0],table[1:]
            headers += header
        tables.append(table)

    writer = csv.writer(options.output)

    if options.header == True:
        writer.writerow(headers)

    key = int
    if options.nonint:
        key = int_if
    for cols in join(tables[0], tables[1], lambda x: key(x[options.j1-1]), lambda x: key(x[options.j2-1])):
        writer.writerow(cols)
