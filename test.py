#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import pymongo
import argparse
import urllib2
import time
import murmur
import liblinear.linear
import liblinear.linearutil
import ast
import tempfile
from collections import namedtuple

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from xml.dom import minidom

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--find', metavar='QUERY',
                        dest='find', type=str, default='{}',
                        help='')
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=str, default='/dev/stdout',
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
        models[model['label']] = liblinear.linearutil.load_model(tmp)
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
    for x in models.keys():
        labels[x] = []
    for ent in cursor:
        for name in labels.keys():
            value = None
            if ent.has_key('labels') and ent['labels'].has_key(name):
                value = ent['labels'][name] if 1 else -1
            labels.setdefault(name, []).append(value)
        vec = {}
        for (x,y) in ent['vector'].items():
            vec[int(x)] = float(y)
        vectors.append((vec, ent['entry']))

    for (name,vals) in labels.items():
        assert len(vectors) == len(vals), [len(vectors), len(vals), name]

    writer = csv.writer(open(options.output, 'w'), delimiter='\t')
    writer.writerow([unicode(x) for x in ['label', 'rev_id', 'predicted', 'coded', 'confidence', 'correct?', 'diff', 'snippet']])
    pn_tuple = namedtuple('pn', 'p n')
    vecs = map(lambda x: x[0], vectors)
    for (lname, labs) in sorted(labels.items(), key=lambda x: x[0]):
        m = models[lname]
        if m == None:
            print >>sys.stderr, lname
            continue
        print lname + ': '

        lab,acc,val = liblinear.linearutil.predict(labs, vecs, m, '-b 1')

        # print performances nad failure cases
        pn = pn_tuple({True: 0, False: 0},
                      {True: 0, False: 0})
        for (i,pred) in enumerate(lab):
            ok = bool(pred) == labs[i]
            res = 'Yes' if ok else 'No'
            if labs[i] == None:
                res = 'Unknown'
            else:
                if pred > 0:
                    pn.p[ok] += 1
                else:
                    pn.n[ok] += 1
            link = 'http://en.wikipedia.org/w/index.php?diff=prev&oldid=%s' % vectors[i][1]['rev_id']
            writer.writerow([unicode(x).encode('utf-8') for x in [lname, vectors[i][1]['rev_id'], bool(pred), labs[i], '%4.3f' % max(val[i]), res, '=HYPERLINK("%s","%s")' % (link,link), ' '.join(vectors[i][1]['content']['added'])[0:50]]])
        print ' accuracy  = %f' % (float(pn.p[True] + pn.n[True]) / sum(pn.p.values() + pn.n.values()))
        prec = float(pn.p[True]) / sum(pn.p.values()) if sum(pn.p.values()) != 0 else float('nan')
        reca = float(pn.p[True]) / (pn.p[True] + pn.n[False]) if pn.p[True] + pn.n[False] != 0 else float('nan')
        print ' precision = %f' % prec
        print ' recall    = %f' % reca
        print ' fmeasure  = %f' % (1.0 / (0.5/prec + 0.5/reca)) if prec != 0 and reca != 0 else float('nan')
        print '', pn, (pn.p[True] + pn.p[False] + pn.n[True] + pn.n[False])
