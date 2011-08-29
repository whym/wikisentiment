#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import argparse
import time
import murmur

from fextract import *
import myutils

def slices(ls, size=2):
    n = int(float(len(ls)) / size + 0.5)
    return map(lambda x: ls[x*size:x*size+size], xrange(0, n))

def extract_features(ent, extractors=[SentiWordNetExtractor('SentiWordNet_3.0.0_20100908.txt', threshold=0.2),
                                      WikiPatternExtractor(),
                                      NgramExtractor(n=2),
                                      NgramExtractor(n=2, lowercase=True),
                                      NgramExtractor(n=1),
                                      NgramExtractor(n=1, lowercase=True),
                                      ]):
    features = {}
    for fx in extractors:
        features[fx.name()] = fx.extract(ent)
    return features

def extract_vector(features, bits):
    vector = {}
    for (fset,vals) in features.items():
        for (f,v) in vals.items():
            h = murmur.string_hash('%s_%s' % (fset, f.encode('utf-8')))
            h = h & (2 ** bits - 1)
            vector[h + 1] = v
    return vector

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--wait', metavar='SECS',
                      dest='wait', type=float, default=0.2,
                      help='')
    parser.add_argument('-b', '--bits', metavar='N',
                        dest='bits', type=int, default=15,
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

    # for each 'entry' in the MongoDB, extract features and put them to 'features'
    db = collection['talkpage_diffs_raw']
    cursor = db.find()
    entries = []
    for ent in cursor:
        features = extract_features(ent)
        vector = myutils.map_key_dict(unicode, extract_vector(features, options.bits))
        # print db.find({'entry.rev_id': ent['entry']['rev_id']}).count()#!
        # print vector,features,ent#!
        ent['vector'] = vector
        ent['features'] = features
        ret = db.save(ent, safe=True)
        if options.verbose:
            print ent['entry']['id']
