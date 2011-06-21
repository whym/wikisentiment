#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import pymongo
import argparse
import urllib2
import time
import murmur
import liblinear
import liblinearutil
import ast

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from xml.dom import minidom

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                      dest='output', type=str, default='output.model',
                      help='')
    parser.add_argument('-f', '--find', metavar='QUERY',
                        dest='find', type=str, default=None,
                        help='')
    parser.add_argument('-d', '--database', metavar='NAME',
                        dest='database', type=unicode, default='wikisentiment',
                        help='')
    parser.add_argument('-H', '--hosts', metavar='HOSTS',
                        dest='hosts', type=str, default='alpha,beta',
                        help='MongoDB hosts')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    options = parser.parse_args()

    # establish MongoDB connection
    options.hosts = options.hosts.split(',')
    master = pymongo.Connection(options.hosts[0])
    slaves = [pymongo.Connection(x) for x in options.hosts[1:]]
    collection = None
    if slaves != []:
        collection = pymongo.MasterSlaveConnection(master, slaves)[options.database]
    else:
        collection = pymongo.database.Database(master, options.database)

    # contruct the training set from 'entry's in the MongoDB
    db = collection['talkpage_diffs_raw']
    query = {'vector': {'$exists': True}}
    if options.find != None:
        query.update(ast.literal_eval(options.find))
    cursor = db.find(query)
    print >>sys.stderr, 'labeld examples: %s out of %s' % (cursor.count(), db.count())
    labels = {}
    vectors = []
    for ent in cursor:
        if not ent.has_key('labels'):
            print >>sys.stderr, 'skip ' + ent['entry']['rev_id']
            continue
        for (name,value) in ent['labels'].items():
            labels.setdefault(name, []).append(value if 1 else -1)
        vec = {}
        for (x,y) in ent['vector'].items():
            vec[int(x)] = float(y)
        vectors.append(vec)

    if not options.output.endswith('.model'):
        options.output += '.model'

    for (lname, labs) in labels.items():
        prob = liblinear.problem(labs, vectors)
        m = liblinearutil.train(prob, liblinear.parameter(''))
        fname = options.output.replace('.', '_%s.' % lname)
        if fname == options.output:
            print >>sys.stderr, 'check --output argument'
        lab,acc,val = liblinearutil.predict(labs, vectors, m)
        liblinear.liblinear.save_model(fname, m)
        print >>sys.stderr, fname
