#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import pymongo
import argparse
import urllib2
import time
import re
import ast

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from xml.dom import minidom
from itertools import groupby
from datetime import datetime

def slices(ls, size=2):
    n = int(float(len(ls)) / size + 0.5)
    return map(lambda x: ls[x*size:x*size+size], xrange(0, n))

diffcella = re.compile('<td class="diff-addedline">(.*?)</td>', re.MULTILINE | re.DOTALL)
diffcellr = re.compile('<td class="diff-deletedline">(.*?)</td>', re.MULTILINE | re.DOTALL)
diffspan  = re.compile('<span class="diffchange">(.*?)</span>', re.MULTILINE | re.DOTALL)
def diffparse(diff):
    ret2 = {}
    for (name,diffcell) in {'added':diffcella, 'removed': diffcellr}.items():
        ret = []
        for x in re.finditer(diffcell, diff):
            seg = x.group(1)
            if diffspan.search(seg) != None:
                spans = []
                for x in diffspan.finditer(seg):
                    spans.append(x.group(1))
                ret.extend(spans)
            else:
                ret.append(seg)
        ret2[name] = ret
    return ret2

def get_revisions(revs):
    url  ='http://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvdiffto=prev&rvprop=size|timestamp|ids|user|comment&format=xml&revids=%s' % ('|'.join(revs))
    while True:
        try:
            print >>sys.stderr, 'fetching %s' % url
            res = urllib2.urlopen(urllib2.Request(url,
                                                  headers={'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11'})).read()
            break
        except urllib2.URLError:
            time.sleep(5)

    ret = []
    pages = minidom.parseString(res).getElementsByTagName('page')
    for p in pages:
        for r in p.getElementsByTagName('rev'):
            ret.append((p, r))
    return ret

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--field', metavar='COLUMN',
                        dest='revfield', type=int, default=2,
                        help='column that contains IDs')
    parser.add_argument('-l', '--labels', metavar='COLUMNS',
                        dest='labels', type=lambda x: [int(y)-1 for y in x.split(',')], default=None,
                        help='columns that contain labels (a label is 0 or 1)')
    parser.add_argument('-D', '--delimiter', metavar='CHARACTER',
                        dest='delimiter', type=lambda x: ast.literal_eval('"'+x+'"'), default=',',
                        help='')
    parser.add_argument('-w', '--wait', metavar='SECS',
                        dest='wait', type=float, default=0.5,
                        help='')
    parser.add_argument('-s', '--slice', metavar='N',
                        dest='slice', type=int, default=10,
                        help='')
    parser.add_argument('-d', '--database', metavar='NAME',
                        dest='database', type=unicode, default='wikisentiment',
                        help='')
    parser.add_argument('-H', '--hosts', metavar='HOSTS',
                        dest='hosts', type=str, default='alpha,beta',
                        help='MongoDB hosts')
    parser.add_argument('-o', '--overwrite',
                        dest='overwrite', action='store_true', default=False,
                        help='')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    parser.add_argument('input')
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

    # load raw table of coded examples
    csv.field_size_limit(1000000000)
    table = list(csv.reader(open(options.input), delimiter=options.delimiter))
    header = []
    if options.labels != None:
        header = [None for x in xrange(0, len(table[0]))]
        for c in options.labels:
            header[c] = table[0][c]
        table = table[1:]
    table_size = len(table)

    options.revfield -= 1

    # prepare HTTP agent for accessing Wikipedia API
    # agent = Agent(reactor)
    # for cols in table:
    #     d = agent.request(
    #         'GET',
    #         'http://en.wikipedia.org/api.php?format=&prop=query&revids=',
    #         Headers({'User-Agent': ['Twisted Web Client Example']}),
    #         None)
    #     d.addCallback(lambda x: )

    # reactor.run()

    # put them into MongoDB (raw information is put into the "entry" attribute)
    db = collection['talkpage_diffs_raw']

    digits = re.compile('\d+')
    table = filter(lambda x: digits.match(x[options.revfield]), table)

    if not options.overwrite:
        # get existing entries
        existings = {}
        for x in db.find({'entry.id.rev_id': {'$exists': True}, 'entry.content': {'$exists': True}}, {'entry.id.rev_id':1, 'entry.content':1}):
            existings[x['entry']['id']['rev_id']] = True
        table = filter(lambda x: not existings.has_key(int(x[options.revfield])), table)

    while len(table) > 0:
        colslices = table[0:options.slice]
        table = table[options.slice:]
        rev2entry = {}
        rev2cols = {}
        for cols in colslices:
            labels = {}
            for (i,lab) in enumerate(header):
                if lab != None:
                    labels[lab] = bool(int(cols[i]))
            revid = int(cols[options.revfield])
            ent = {'entry': {'id': {'rev_id': revid,
                             }}}
            if options.labels != None:
                ent.update({'labels': labels})
            rev2entry[revid] = ent
            rev2cols[int(cols[options.revfield])] = cols

        # call API to get content etc
        revs = get_revisions([str(x) for x in rev2cols.keys()])

        keyfunc = lambda x: x[1].childNodes[0].attributes.has_key('notcached')
        revs.sort(key=keyfunc)
        groups = dict(groupby(revs, key=keyfunc))
        groups.setdefault(False, [])
        groups.setdefault(True,  [])
        revs = list(groups[False])
        queued = [rev2cols[int(x[1].attributes['revid'].value)] for x in groups[True]]
        table = queued + table
        print >>sys.stderr, "received %d, queued %d (%d/%d)" % (len(revs), len(queued), len(revs) + db.count(), table_size)

        for (page,rev) in revs:
            ent = rev2entry[int(rev.attributes['revid'].value)]
            ent['entry'].update({'title'  : page.attributes['title'].value})
            for (orig, (tar,func)) in {'user': ('sender', unicode),
                                       'comment': ('comment', unicode),
                                       'timestamp': ('timestamp', lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ')),
                                       'size': ('size', int)
                                       }.items():
                ent['entry'][tar] = func(rev.attributes[orig].value)
            if len(rev.childNodes) == 0 or len(rev.childNodes[0].childNodes) == 0:
                print >>sys.stderr, "empty diff: %s" % rev.toxml()
            else:
                ent['entry']['content'] = diffparse(rev.childNodes[0].childNodes[0].data)
        for x in rev2entry.values():
            db.update({'entry.id.rev_id': x['entry']['id']['rev_id']}, x, upsert=True, safe=True)
        time.sleep(options.wait)


        # extract features
        # here will be function calls of feature extraction and online training
        # currently: run ./extract_features.py , ./train.py (and ./test.py) separately

