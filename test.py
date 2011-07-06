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
import tempfile

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from xml.dom import minidom

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--find', metavar='QUERY',
                        dest='find', type=str, default='{}',
                        help='')
    parser.add_argument('-m', '--model', metavar='QUERY',
                        dest='model', type=str, default='{}',
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

    # load models for each label
    db = collection['models']
    models = {}
    for model in db.find(ast.literal_eval(options.model)):
        tmp = tempfile.mktemp(prefix=model['label'].replace('/','_'))
        f = open(tmp, 'w')
        f.write(model['raw_model'])
        models[model['label']] = liblinearutil.load_model(tmp)
        f.close()

    # contruct the testing set from 'entry's in the MongoDB
    # construct vectors for libsvm
    db = collection['talkpage_diffs_raw']
    query = {'vector': {'$exists': True}}
    query.update(ast.literal_eval(options.find))
    cursor = db.find(query)
    print >>sys.stderr, 'labeld examples: %s out of %s' % (cursor.count(), db.count())

    vectors = []
    labels = {}
    for ent in cursor:
        if not ent.has_key('labels'):
            print >>sys.stderr, 'skip ' + ent['entry']['rev_id']
            continue
        for (name,value) in ent['labels'].items():
            labels.setdefault(name, []).append(value if 1 else -1)
        vec = {}
        for (x,y) in ent['vector'].items():
            vec[int(x)] = float(y)
        vectors.append((vec, ent['entry']['rev_id']))

    vecs = map(lambda x: x[0], vectors)
    for (lname, labs) in labels.items():
        m = models[lname]
        if m == None:
            print >>sys.stderr, lname
            continue
        print lname + ': '

        lab,acc,val = liblinearutil.predict(labs, vecs, m, '-b 1')

        # print failure cases
        for (i,pred) in enumerate(lab):
            ng = bool(pred) != labs[i]
            if ng or options.verbose:
                print vectors[i][1], bool(pred), labs[i], '%4.3f' % max(val[i]), 'ng' if ng else 'ok'

        # TODO: f-measure
