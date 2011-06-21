#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import pymongo
import argparse
import urllib2
import time
import murmur

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from xml.dom import minidom

from fextract import *

def slices(ls, size=2):
    n = int(float(len(ls)) / size + 0.5)
    return map(lambda x: ls[x*size:x*size+size], xrange(0, n))

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--wait', metavar='SECS',
                      dest='wait', type=float, default=0.2,
                      help='')
    parser.add_argument('-b', '--bits', metavar='N',
                        dest='bits', type=int, default=14,
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

    # for each 'entry' in the MongoDB, extract features and put them to 'features'
    extractors = [SentiWordNetExtractor('SentiWordNet_3.0.0_20100908.txt'), WikiSyntaxExtractor()]
    db = collection['talkpage_diffs_raw']
    cursor = db.find()
    entries = []
    for ent in cursor:
        features = {}
        for fx in extractors:
            features[fx.name()] = fx.extract(ent)
        entries.append((ent,features))
    for (ent,features) in entries:
        vector = {}
        for (fset,vals) in features.items():
            for (f,v) in vals.items():
                h = murmur.string_hash('%s_%s' % (fset,f))
                h = h & (2 ** options.bits - 1)
                vector[str(h)] = v
        ret = (ent['entry']['rev_id'], db.update({'entry.rev_id': ent['entry']['rev_id']}, {'$set': {'vector': vector, 'features': features }}, safe=True))
        if options.verbose:
            print ret

