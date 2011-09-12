#! /usr/bin/env python
# -*- coding: utf-8 -*-
# 

# show usernames
# show generated date/time

import os
import argparse
import sys
import csv
import urllib2
import re
try:
    import liblinear.linear
    import liblinear.linearutil
except ImportError:
    import liblinear
    import liblinearutil
    liblinear.linearutil = liblinearutil
    liblinear.linear = liblinear
import ast
import tempfile
from datetime import datetime, timedelta
import myutils
import wikilove_revs
import extract_features
import test

def difflink(n, text):
    return '<a href="http://en.wikipedia.org/w/index.php?diff=prev&oldid=%d">%s</a>' % (n, text)

def userpagelink(name, text=None):
    if text == None:
        text = name
    return '<a href="http://en.wikipedia.org/wiki/User:%s">%s</a>' % (name, text)

def contributionslink(name, timestamp, text):
    return '<a href="http://en.wikipedia.org/wiki/Special:Contributions/%s?offset=%s&dir=prev">%s</a>' % (name, timestamp, text)

def write_list(output, entries):
    print >>output, '<ul>'
    for (ent,preds) in entries:
        
        if ent.raw.rev_id != None:
            print >>output, '<li><p>%s %s→%s</p><blockquote title="%s">%s</blockquote>' % (contributionslink(ent.raw.sender_name, ent.raw.timestamp, str(myutils.parse_wikidate(ent.raw.timestamp))), userpagelink(ent.raw.sender_name), userpagelink(ent.raw.receiver_name), urllib2.quote(repr(ent.features), safe='{}\': '), difflink(ent.raw.rev_id, ent.raw.message))
        else:
            print >>output, '<li><p>%s</p><blockquote title="%s">%s</blockquote>' % (ent.raw.timestamp, repr(ent.features), ent.raw.message)
        print >>output, '<p class="prediction">'
        for (i,(lname,score)) in enumerate(zip(labels, preds)):
            colors = [1.0] * 3
            for j in xrange(0,3):
                if i % 3 != j:
                    colors[j] -= score
            print >>output, '<span style="color: #%s">%s: %.4f</span><br/>' % (''.join([myutils.fraction_to_color_code(x) for x in colors]), lname, score)
        print >>output, '</p></li>'

    print >>output, '</ul>'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-w', '--window', metavar='SECS',
                        dest='window', type=int, default=10,
                        help='')
    parser.add_argument('-t', '--threshold', metavar='FRACTION',
                        dest='threshold', type=float, default=0.5,
                        help='')
    parser.add_argument('-b', '--bits', metavar='N',
                        dest='bits', type=int, default=15,
                        help='')
    parser.add_argument('-l', '--limit', metavar='N',
                        dest='limit', type=int, default=100,
                        help='')
    parser.add_argument('-s', '--start', metavar='DATE',
                        dest='start', type=str, default='20110101000000',
                        help='')
    parser.add_argument('-e', '--end', metavar='DATE',
                        dest='end',   type=str, default='30110101000000',
                        help='')
    parser.add_argument('-d', '--db', metavar='DBNAME', required=True,
                        dest='db', type=str, default='enwiki-p',
                        help='target wiki name')
    parser.add_argument('-H', '--host', metavar='HOST',
                        dest='host', type=str, default='',
                        help='mysql host name')
    parser.add_argument('-m', '--model', metavar='QUERY',
                        dest='model', type=str, default='{}',
                        help='')
    parser.add_argument('-D', '--database', metavar='NAME',
                        dest='database', type=unicode, default='wikisentiment',
                        help='')
    parser.add_argument('--mongo-hosts', metavar='HOSTS',
                        dest='hosts', type=str, default='alpha,beta',
                        help='MongoDB hosts')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    options = parser.parse_args()

    # establish MongoDB connection
    collection = myutils.get_mongodb_collection(options.hosts, options.database)

    # load models for each label
    models = test.load_models(collection['models'], ast.literal_eval(options.model))

    cursor = myutils.get_mysql_connection(options.host, options.db).cursor()
    # contruct the testing set from the MediaWiki table
    vectors = []
    for ent in wikilove_revs.get_entries(cursor, options.start, options.end, options.window, options.limit, newest=True):
        features = extract_features.extract_features({'entry': {'content': {'added': [ent.others.message], 'removed':[]},
                                                                'comment': ''}})
        vector = myutils.map_key_dict(int, extract_features.extract_vector(features, options.bits))
        if ent.receiver_id != ent.sender_id:
            vectors.append(myutils.entry_t(ent, features, vector))

    labels = sorted(models.keys())
    
    vecs = [x.vector for x in vectors]
    predictions = [[[] for y in xrange(0, len(labels))] for x in xrange(0,len(vectors))]
    for (n,lname) in enumerate(labels):
        lab,_,val = liblinear.linearutil.predict([0]*len(vecs), vecs, models[lname], '-b 1')
        for (i,(pred,score)) in enumerate(zip(lab,val)):
            predictions[i][n] = score[1] # get the confidence for the label being 'True'

    print >>options.output, '<style type="text/css">.prediction{text-align: right;} td{vertical-align: top;} li{border: 1px solid; list-style: none inside; margin: 0.2em;} ul{padding: 0;} blockquote{ font: normal italic  100% serif; }</style>'
    print >>options.output, '<body style="background: #EEE;">Generated at %s.' % str(datetime.now())
    print >>options.output, '<table style="background: white; width: 100%"><tr>'
    for (i,x) in enumerate(labels):
        print >>options.output, '<th>%s: %d out of %d (>%f)</th>' % (x, len(filter(lambda x: x[i] > options.threshold, predictions)), len(predictions), options.threshold)
    uncertains = [x for x in zip(vectors, predictions) if not any([y >= options.threshold for y in x[1]])]
    print >>options.output, '<th>uncertain: %d out of %d</th>' % (len(uncertains), len(predictions))
    print >>options.output, '</tr><tr>'
    sorteds = [sorted(x) for x in predictions]
    for i in xrange(0, len(labels)):
        color = ['33'] * 3
        color[i % 3] = '99'
        color = ''.join(color)
        print >>options.output, '<td style="color: #%s">' % color
        gaps = [[x[i] - y for y in s] for (x,s) in zip(predictions, sorteds)]
        write_list(options.output, [tuple(x[0:2]) for x in sorted(zip(vectors, predictions, gaps), key=lambda x: -x[2][-2] if x[2][-2] > 0 else -x[2][-1]) if x[1][i] >= options.threshold ])
        print >>options.output, '</td>'
    print >>options.output, '<td>'
    write_list(options.output, uncertains)
    print >>options.output, '</td></tr></table></body>'
