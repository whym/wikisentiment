#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import argparse
import time
import liblinear.linear
import liblinear.linearutil
import ast
import tempfile
import myutils

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
    collection = myutils.get_mongodb_collection(options.hosts, options.database)

    # contruct the training set from 'entry's in the MongoDB
    db = collection['talkpage_diffs_raw']
    query = {'labels': {'$exists': True},
             'vector': {'$exists': True}}
    query.update(ast.literal_eval(options.find))
    cursor = db.find(query)
    print >>sys.stderr, 'using labeld examples: %s out of %s' % (cursor.count(), db.count())
    labels = {}
    vectors = []
    entries = []
    for ent in cursor:
        if not ent.has_key('labels'):
            print >>sys.stderr, 'skip %s' % ent['entry']['id']
            continue
        vec = myutils.map_key_dict(int, ent['vector'])
        if len(vec.items()) == 0:
            print >>sys.stderr, 'empty %s' % ent['entry']['id']
            #continue
        vectors.append(vec)
        entries.append(ent)
        for (name,value) in ent['labels'].items():
            labels.setdefault(name, []).append(value if 1 else -1)
        if options.verbose:
            print >>sys.stderr, str(ent['entry']['id'])

    if options.verbose:
        print >>sys.stderr, 'vectors loaded %s' % len(vectors)

    for (name,vals) in labels.items():
        assert len(vectors) == len(vals), [len(vectors), len(vals), name]

    # train and output models
    db = collection['models']
    for (lname, labs) in labels.items():
        prob = liblinear.linear.problem(labs, vectors)
        if options.verbose:
            print >>sys.stderr, '"%s" problem constructed' % lname
        m = liblinear.linearutil.train(prob, liblinear.linear.parameter('-s 6 -c 0.8'))
        if options.verbose:
            print >>sys.stderr, '"%s" model trained' % lname

        lab,acc,val = liblinear.linearutil.predict(labs, vectors, m)

        tmp = tempfile.mktemp(prefix=lname.replace('/','_'))
        liblinear.linearutil.save_model(tmp, m)
        print >>sys.stderr, '%s: %s' % (lname, tmp)

        model = open(tmp).read()

        # write weights
        for (i,line) in enumerate(model.split('\n')[6:]):
            None #TODO: 

        q = {'label': lname}

        q.update(ast.literal_eval(options.model))
        r = {'label': lname,
             'raw_model': model,
             #'model': features,
             'date': time.time() }
        r.update(ast.literal_eval(options.model))
        print db.update(q, r, upsert=True, safe=True)
