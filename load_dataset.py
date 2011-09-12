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
import myutils
from xml.dom import minidom
from itertools import groupby
from datetime import datetime

global idbase
idbase = 0

def slices(ls, size=2):
    n = int(float(len(ls)) / size + 0.5)
    return map(lambda x: ls[x*size:x*size+size], xrange(0, n))

diffcella = re.compile('<td class="diff-addedline">(.*?)</td>', re.MULTILINE | re.DOTALL)
diffcellr = re.compile('<td class="diff-deletedline">(.*?)</td>', re.MULTILINE | re.DOTALL)
diffspan  = re.compile('<span class="diffchange">(.*?)</span>', re.MULTILINE | re.DOTALL)
def diffparse(diff):
    ret2 = {}
    for (name,diffcell) in {'added': diffcella, 'removed': diffcellr}.items():
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
    parser.add_argument('-i', '--id-fields', metavar='COLUMNS',
                        dest='idfields', type=lambda x: [int(y)-1 for y in x.split(',')], default=None,
                        help='columns that contain labels (a label is 0 or 1)')
    parser.add_argument('-t', '--text-field', metavar='COLUMN',
                        dest='textfield', type=int, default=None,
                        help='column that contains text')
    parser.add_argument('-l', '--label-fields', metavar='COLUMNS',
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
    collection = myutils.get_mongodb_collection(options.hosts, options.database)

    # load raw table of coded examples
    csv.field_size_limit(1000000000)
    table = list(csv.reader(open(options.input, 'rU'), delimiter=options.delimiter))
    header = []
    if options.labels != None or options.idfields != None:
        header = [None for x in xrange(0, len(table[0]))]
        for c in options.labels + options.idfields:
            header[c] = table[0][c]
        table = table[1:]
    table_size = len(table)

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

    if options.idfields != None:
        digits = re.compile('\d+')
        table = filter(lambda x: reduce(lambda s,y: s and y, [digits.match(x[i]) for i in options.idfields]), table)

    if options.overwrite and options.idfields != None:
        # get existing entries
        existings = {}
        query = {}
        for i in options.idfields:
            query['entry.id.' + header[i]] = {'$exists': True}
        query['entry.content'] = {'$exists': True}
        for x in db.find(query, {'entry.id': 1, 'entry.content': 1}):
            existings[tuple([x['entry']['id'][header[i]] for i in options.idfields])] = True
        table = filter(lambda x: not existings.has_key(tuple([myutils.int_if(x[i]) for i in options.idfields])), table)
    if options.overwrite and options.idfields == None:
        print >>sys.stderr, 'overwrite requires idfields'

    def extract_entries(rows):
        for cols in rows:
            labels = {}
            for (i,lab) in enumerate(header):
                if lab != None and (cols[i] == '1' or cols[i] == '0'):
                    labels[lab] = bool(int(cols[i]))
            ids = []
            if options.idfields != None:
                ids = [myutils.int_if(cols[i]) for i in options.idfields]
            else:
                ids = [idbase]
            
            text = None
            ent = {'entry': {'id': dict(zip([header[i] for i in options.idfields], ids))}}
            if options.labels != None and labels != {}:
                ent.update({'labels': labels})
            ent['entry'].update({'content': {'added': [],
                                             'removed': []},
                                 'comment': ''})
            if options.textfield != None:
                ent['entry']['content']['added'] = [cols[options.textfield].decode('UTF-8')]
            yield (cols,ent)

    if options.textfield:
        for (cols,ent) in extract_entries(table):
            idbase += 1
            db.update({'entry.id': ent['entry']['id']}, ent, upsert=True, safe=True)
            if options.verbose:
                print >>sys.stderr, ent['entry']['id'], len(ent['entry']['content']['added']), ent
        print >>sys.stderr, '%d entries added' % len(table)
    else:
        while len(table) > 0:
            colslices = table[0:options.slice]
            table = table[options.slice:]
            rev2entry = {}
            rev2cols = {}
            for (cols,ent) in extract_entries(colslices):
                revid = ent['entry']['id']['rev_id']
                rev2entry[revid] = ent
                rev2cols[revid] = cols
    
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
                db.update({'entry.id': x['entry']['id']}, x, upsert=True, safe=True)
            time.sleep(options.wait)
    

        # extract features
        # here will be function calls of feature extraction and online training
        # currently: run ./extract_features.py , ./train.py (and ./test.py) separately

