#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import argparse
import sys
import ast

def join(table1, table2, j1, j2):
    table1 = sorted(table1, key=j1)
    table2 = sorted(table2, key=j2)

    len1 = len(table1[0])
    len2 = len(table2[0])

    while True:
        #print j1(table1[0]), j2(table2[0])
        if len(table2) > 0 and j1(table1[0]) == j2(table2[0]):
            yield table1[0] + table2[0]
            table2 = table2[1:]
        while len(table2) > 0 and j1(table1[0]) > j2(table2[0]):
            yield ['']*len1 + table2[0]
            table2 = table2[1:]
        if len(table2) == 0 or j1(table1[0]) < j2(table2[0]):
            yield table1[0] + ['']*len2
            table1 = table1[1:]
            if len(table1) == 0:
                break
            table1 = table1[1:]
    for cols2 in table2:
        yield ['']*len1 + cols2

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-i', '--input', metavar='FILE',
                        dest='input', type=lambda x: open(x, 'rU'), default=sys.stdin,
                        help='')
    parser.add_argument('-f', '--delimiter-in', metavar='CHARACTER',
                        dest='delimiterin', type=lambda x: ast.literal_eval('"'+x+'"'), default=',',
                        help='')
    parser.add_argument('-t', '--delimiter-out', metavar='CHARACTER',
                        dest='delimiterout', type=lambda x: ast.literal_eval('"'+x+'"'), default=',',
                        help='')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    options = parser.parse_args()

    writer = csv.writer(options.output, delimiter=options.delimiterout)
    for row in  csv.reader(options.input, delimiter=options.delimiterin):
        writer.writerow(row)
