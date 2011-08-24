#! /usr/bin/env python
# -*- coding: utf-8 -*-
# 

import oursql
import os
import argparse
import sys
import csv
import re
from collections import namedtuple
from datetime import datetime, timedelta
from myutils import parse_wikidate, format_wikidate, wikilove_t

wikilovelog_t = namedtuple('WikiLoveLog', 'id timestamp senderid senderreg sendercount receiverid receiverreg receivercount wltype subject message')

def get_entries(cursor, start, end, window, limit=100000):
    cursor.execute('''
          SELECT *
            FROM wikilove_log l
          WHERE
            l.wll_timestamp BETWEEN ? AND ?
        ;
    ''', (start, end))

    ls = [wikilovelog_t(*x) for x in list(cursor)]
    #anons = filter(lambda x: x.senderid == 0, ls) # wikilove_log contains no messages sent by anons (?)
    regis = filter(lambda x: x.senderid != 0, ls) 
    output = []
    for tup in regis:
        cursor.execute('''
           SELECT r.rev_id
             FROM revision r
             WHERE r.rev_user = ?
               AND r.rev_timestamp BETWEEN ? AND ?
           LIMIT 2
           ;
        ''', (tup.senderid,
              format_wikidate(parse_wikidate(tup.timestamp)+timedelta(seconds=-window)),
              format_wikidate(parse_wikidate(tup.timestamp)+timedelta(seconds=+window))))
        ls = list(cursor)
        if ls == None or len(ls) == 0 or len(ls[0]) == 0:
            print >>sys.stderr, '%s: no candidate: %s' % (tup.id, str(tup))
            ls = [(None,)]
        if len(ls) >= 2:
            print >>sys.stderr, '%s: 2 or more candidates: %s' % (tup.id, ls)
            ls = [(None,)]
        output.append(wikilove_t(rev_id=ls[0][0],
                                 sender_id=tup.senderid,
                                 sender_name=None,
                                 receiver_id=tup.receiverid,
                                 receiver_name=None,
                                 timestamp=tup.timestamp,
                                 others=tup))
    return output

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-w', '--window', metavar='SECS',
                        dest='window', type=int, default=10,
                        help='')
    parser.add_argument('-s', '--start', metavar='DATE',
                        dest='start', type=str, default='20110101000000',
                        help='')
    parser.add_argument('-e', '--end', metavar='DATE',
                        dest='end',   type=str, default='30110101000000',
                        help='')
    parser.add_argument('-d', '--db', metavar='DBNAME', required=True,
                        dest='db', type=str,
                        help='target wiki name')
    options = parser.parse_args()
    options.db = options.db.replace('_','-')

    host = options.db + '.rrdb.toolserver.org'
    conn = oursql.connect(host = host,
                          read_default_file=os.path.expanduser('~/.my.cnf'),
                          db = options.db.replace('-','_'),
                          charset=None,
                          use_unicode=False)

    cursor = conn.cursor()
    output = get_entries(cursor, options.start, options.end, options.window, 1000000)
    writer = csv.writer(options.output, delimiter='\t')
    for ent in output:
        writer.writerow([unicode(x.decode('utf-8') if type(x) == str else x).encode('utf-8') for x in ent[0:-1]+ent[-1]])
