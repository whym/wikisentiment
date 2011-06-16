#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import pymongo
import argparse
import urllib2
import time

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from xml.dom import minidom

def slices(ls, size=2):
    n = int(float(len(ls)) / size + 0.5)
    return map(lambda x: ls[x*size:x*size+size], xrange(0, n))

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--wait', metavar='SECS',
                      dest='wait', type=float, default=0.2,
                      help='')
    parser.add_argument('-s', '--slice', metavar='N',
                        dest='slice', type=float, default=40,
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
    parser.add_argument('input')
    options = parser.parse_args()

    # load raw table of coded examples
    csv.field_size_limit(1000000000)
    table = list(csv.reader(open(options.input)))
    header = [None,None] + table[0][2:6]
    table = table[1:]

    # establish MongoDB connection
    options.hosts = options.hosts.split(',')
    master = pymongo.Connection(options.hosts[0])
    slaves = [pymongo.Connection(x) for x in options.hosts[1:]]
    collection = None
    if slaves != []:
        collection = pymongo.MasterSlaveConnection(master, slaves)[options.database]
    else:
        collection = pymongo.database.Database(master, options.database)

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
    ls = []
    for colslices in slices(table, options.slice):
        ls = []
        revs = []
        for cols in colslices:
            labels = {}
            for (i,lab) in enumerate(header):
                if lab != None:
                    labels[lab] = bool(int(cols[i]))
            ent = {'entry': {'title': cols[0],
                             'receiver': cols[0],
                             'rev_id': int(cols[1]),
                             },
                   'labels': labels}
            ls.append(ent)
            revs.append(cols[1])
        # call API to get content etc
        url  ='http://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=timestamp|ids|user|content&format=xml&revids=' + '|'.join(revs) 
        print >>sys.stderr, "fetching " + url
        res = urllib2.urlopen(urllib2.Request(url,
                                              headers={'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11'})).read()
        revs = minidom.parseString(res).getElementsByTagName('rev')
        print >>sys.stderr, "received %d" % len(revs)
        for (i,rev) in enumerate(revs):
            ls[i]['entry']['sender'] = rev.attributes['user'].value
            if len(rev.childNodes) == 0:
                print >>sys.stderr, "empty content: %s" % rev.toxml()
                ls[i]['entry']['content'] = ''
            else:
                ls[i]['entry']['content'] = rev.childNodes[0].data
        db.insert(ls)
        time.sleep(options.wait)


    # extract features
