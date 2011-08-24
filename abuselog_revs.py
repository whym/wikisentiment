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
from collections import namedtuple

abuselog_t = namedtuple('AbuseLog', 'id filter userid username action actions var_dump timestamp namespace title')

def parse_wikidate(x):
    return datetime.strptime(str(x), '%Y%m%d%H%M%S')

def format_wikidate(x):
    return datetime.strftime(x, '%Y%m%d%H%M%S')

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

    ls = [abuselog_t(*x) for x in list(cursor)]
    anons = filter(lambda x: x.userid == 0, ls)
    regis = filter(lambda x: x.userid != 0, ls) 
    output = []
    for tup in regis:
        cursor.execute('''
           SELECT r.rev_id
             FROM revision r
             WHERE r.rev_user = ?
               AND r.rev_timestamp BETWEEN ? AND ?
           LIMIT 1
           ;
        ''', (tup.userid,
              format_wikidate(parse_wikidate(tup.timestamp)+timedelta(seconds=-1)),
              format_wikidate(parse_wikidate(tup.timestamp)+timedelta(seconds=1))))
        ls = list(cursor)
        if ls == None or len(ls) == 0 or len(ls[0]) == 0:
            ls = [(None,)]
        output.append((ls[0][0], tup))
    for tup in anons:
        cursor.execute('''
           SELECT r.rev_id
             FROM revision r
             WHERE r.rev_user_text = ?
               AND r.rev_timestamp BETWEEN ? AND ?
           LIMIT 1
           ;
        ''', (tup.username,
              format_wikidate(parse_wikidate(tup.timestamp)+timedelta(seconds=-1)),
              format_wikidate(parse_wikidate(tup.timestamp)+timedelta(seconds=1))))
        ls = list(cursor)
        if ls == None or len(ls) == 0 or len(ls[0]) == 0:
            ls = [(None,)]
        output.append((ls[0][0], tup))
    for (rev,tup) in output:
        options.output.write('\t'.join([str(x) for x in [rev,tup.username,tup.userid,tup.timestamp,tup.namespace,tup.title]]))
        options.output.write('\n')
