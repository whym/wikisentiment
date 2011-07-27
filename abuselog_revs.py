#! /usr/bin/env python
# -*- coding: utf-8 -*-
# 

import oursql
import os
import argparse
import sys
import csv
import urllib2
import re
from datetime import datetime, timedelta

def parse_wikidate(x):
    return datetime.strptime(str(x), '%Y%m%d%H%M%S')

def format_wikidate(x):
    return datetime.strftime(x, '%Y%m%d%H%M%S')

def title2pageid(cursor, title, namespace=0):
    cursor.execute('''
          SELECT p.page_id, page_is_redirect
            FROM page p
            WHERE
              p.page_title = ?
              AND p.page_namespace = ?
        ;
    ''', (title,namespace))
    ls = list(cursor)
    if len(ls) == 0:
        return (None,None)
    return tuple(ls[0])
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-f', '--filter', metavar='N',
                        dest='filternum', type=int, default=423,
                        help='')
    parser.add_argument('-d', '--db', metavar='DBNAME', required=True,
                        dest='db', type=str, default='hywiki-p',
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
    cursor.execute('''
          SELECT *
            FROM abuse_filter_log a
            WHERE a.afl_filter = ?
              AND a.afl_action = "edit"
        ;
    ''', (options.filternum,))

    ls = list(cursor)
    anons = filter(lambda x: x[2] == 0, ls)
    regis = filter(lambda x: x[2] != 0, ls) 
    output = []
    for tup in regis:
        _,_,senderid,sendername,_,_,_,timestamp = tup[0:8]
        cursor.execute('''
           SELECT r.rev_id
             FROM revision r
             WHERE r.rev_user = ?
               AND r.rev_timestamp BETWEEN ? AND ?
           LIMIT 1
           ;
        ''', (senderid,
              format_wikidate(parse_wikidate(timestamp)+timedelta(seconds=-1)),
              format_wikidate(parse_wikidate(timestamp)+timedelta(seconds=1))))
        ls = list(cursor)
        if ls == None or len(ls) == 0 or len(ls[0]) == 0:
            ls = [(None,)]
        output.append(list(ls[0] + tup))
    for tup in anons:
        _,_,senderid,sendername,_,_,_,timestamp = tup[0:8]
        cursor.execute('''
           SELECT r.rev_id
             FROM revision r
             WHERE r.rev_user_text = ?
               AND r.rev_timestamp BETWEEN ? AND ?
           LIMIT 1
           ;
        ''', (sendername,
              format_wikidate(parse_wikidate(timestamp)+timedelta(seconds=-1)),
              format_wikidate(parse_wikidate(timestamp)+timedelta(seconds=1))))
        ls = list(cursor)
        if ls == None or len(ls) == 0 or len(ls[0]) == 0:
            ls = [(None,)]
        output.append(list(ls[0] + tup))
    for tup in output:
        rev,_,_,senderid,sendername,_,_,_,timestamp,ns,title = tup[0:11]
        options.output.write('\t'.join([str(x) for x in [rev,sendername,senderid,timestamp,ns,title]]))
        options.output.write('\n')
