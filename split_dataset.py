#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import argparse
import time
import ast
import random

def splits(rates, size, substs=xrange(0,2**16)):
    segsize = size / sum(rates)
    n = 0
    i = 0
    ret = []
    while True:
        ret += (segsize * rates[i]) * [substs[i]]
        i += 1
        if len(ret) >= size or i >= len(rates):
            break
    if len(ret) < size:
        ret += (size - len(ret)) * [ret[-1]]
    return ret[0:size]

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--find', metavar='QUERY',
                        dest='find', type=str, default='{\'vector\': {\'$exists\': True}}',
                        help='')
    parser.add_argument('-s', '--split', metavar='RATES',
                        dest='split', type=str, default='1,1,1,1,1',
                        help='')
    parser.add_argument('-d', '--database', metavar='NAME',
                        dest='database', type=unicode, default='wikisentiment',
                        help='')
    parser.add_argument('-H', '--hosts', metavar='HOSTS',
                        dest='hosts', type=str, default='alpha,beta',
                        help='MongoDB hosts')
    parser.add_argument('-r', '--random-seed', metavar='SEED',
                        dest='seed', type=int, default=int(time.time()),
                        help='random number seed')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    options = parser.parse_args()

    random.seed(options.seed)

    # establish MongoDB connection
    collection = myutils.get_mongodb_collection(options.hosts, options.database)

    # contruct the training set from 'entry's in the MongoDB
    db = collection['talkpage_diffs_raw']
    cursor = db.find(ast.literal_eval(options.find))
    print >>sys.stderr, 'target entries: %s out of %s' % (cursor.count(), db.count())
    numbers = splits([int(x) for x in options.split.split(',')], cursor.count())
    random.shuffle(numbers)
    assert cursor.count() == len(numbers), [cursor.count(), len(number)]
    for (n,ent) in enumerate(sorted(cursor, key=lambda x: x['entry']['id'])):
        print (numbers[n], str(ent['entry']['id']), db.update({'entry.id': ent['entry']['id']}, {'$set': {'internal.slice': numbers[n]}}, safe=True))
