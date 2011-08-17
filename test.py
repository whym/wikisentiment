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

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--find', metavar='QUERY',
                        dest='find', type=str, default='{}',
                        help='')
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-l', '--snippet-len', metavar='N',
                        dest='snippetlen', type=int, default=50,
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
    parser.add_argument('-a', '--aggregate',
                        dest='aggregate', action='store_true', default=False,
                        help='aggregate multi-labeled predictions with rev_id')
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

    labels = sorted(labels.items(), key=lambda x: x[0])

    writer = csv.writer(options.output, delimiter='\t')
    if options.aggregate:
        writer.writerow([unicode(x) for x in ['label', 'rev_id'] + [x[0] for x in labels] + ['diff', 'snippet']])
    else:
        writer.writerow([unicode(x) for x in ['rev_id', 'predicted', 'coded', 'confidence', 'correct?', 'diff', 'snippet']])
    pn_tuple = namedtuple('pn', 'p n')
    vecs = map(lambda x: x[0], vectors)
    output = {}
    for (lname, labs) in labels:
        m = models[lname]
        if m == None:
            print >>sys.stderr, lname
            continue
        print lname + ': '

        lab,acc,val = liblinear.linearutil.predict(labs, vecs, m, '-b 1')

        # print performances and failure cases
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
            link = 'http://enwp.org/?diff=prev&oldid=%s' % vectors[i][1]['rev_id']
            ls = [lname,
                  vectors[i][1]['rev_id'],
                  bool(pred),
                  labs[i],
                  '%4.3f' % max(val[i]),
                  res,
                  '=HYPERLINK("%s","%s")' % (link,link),
                  '"' + (' '.join(vectors[i][1]['content']['added'])[0:options.snippetlen]) + '"' if vectors[i][1].has_key('content') else '(empty)']
            output.setdefault(ls[1],[]).append(ls)
        numcorrect = pn.p[True] + pn.n[True]
        numwrong   = pn.p[False] + pn.n[False]
        if options.verbose:
            print ' accuracy  = %f (%d/%d)' % (float(numcorrect) / (numcorrect + numwrong) if numcorrect + numwrong > 0 else float('nan'),
                                               numcorrect,
                                               (numcorrect + numwrong))
            prec = float(pn.p[True]) / sum(pn.p.values()) if sum(pn.p.values()) != 0 else float('nan')
            reca = float(pn.p[True]) / (pn.p[True] + pn.n[False]) if pn.p[True] + pn.n[False] != 0 else float('nan')
            print ' precision = %f' % prec
            print ' recall    = %f' % reca
            print ' fmeasure  = %f' % (1.0 / (0.5/prec + 0.5/reca)) if prec != 0 and reca != 0 else float('nan')
            print '', pn, (pn.p[True] + pn.p[False] + pn.n[True] + pn.n[False])

    if options.aggregate:
        for (rev_id, s) in output.items():
            writer.writerow([unicode(x).encode('utf-8') for x in (s[0][1:2] + [unicode(x[2]) for x in s] + s[0][-2:])])
    else:
        for (rev_id, s) in output.items():
            for ls in s:
                writer.writerow([unicode(x).encode('utf-8') for x in ls])
