#! /usr/bin/env python
# -*- coding: utf-8 -*-
# 

from datetime import datetime
from collections import namedtuple
import pymongo
import oursql
import os

wikilove_t = namedtuple('WikiLove', 'rev_id sender_id sender_name receiver_id receiver_name timestamp others')
entry_t    = namedtuple('Entry', 'raw features vector')
pn_t       = namedtuple('PN', 'p n')
prediction_t = namedtuple('Prediction', 'label confidence')

def map_key_dict(mapper, dic):
    ret = {}
    for (x,y) in dic.items():
        ret[mapper(x)] = y
    return ret

def int_if(x):
    try:
        return int(x)
    except ValueError:
        return x

def comment_out(line):
    cstart = line.find('#')
    if cstart >= 0:
        cc = line.find('\\#')
        if cstart == 0 or cc + 1 != cstart:
            line = line[0:cstart]
    return line

def fraction_to_color_code(x):
    try:
        return ('%02X' % int(255 * x))
    except ValueError:
        return '00'

def parse_wikidate(x):
    return datetime.strptime(str(x), '%Y%m%d%H%M%S')

def format_wikidate(x):
    return datetime.strftime(x, '%Y%m%d%H%M%S')

def parse_host_port(host, port=3306):
    a = host.split(':')
    if len(a) == 2:
        host,port = a[0],int(a[1])
    return host,port

def get_mongodb_collection(hoststr, dbstr):
    hoststr = hoststr.split(',')
    master = pymongo.Connection(hoststr[0])
    slaves = [pymongo.Connection(x) for x in hoststr[1:]]
    collection = None
    if slaves != []:
        collection = pymongo.MasterSlaveConnection(master, slaves)[dbstr]
    else:
        collection = pymongo.database.Database(master, dbstr)
    return collection

def get_mysql_connection(hoststr, dbstr):
    dbstr = dbstr.replace('_','-')

    port = 3306
    if hoststr == '':
	hoststr = dbstr + '.rrdb.toolserver.org'
    else:
        hoststr,port = parse_host_port(hoststr)
    conn = oursql.connect(host = hoststr,
                          read_default_file=os.path.expanduser('~/.my.cnf'),
                          db = dbstr.replace('-','_'),
                          charset=None,
                          port=port,
                          use_unicode=False)
    return conn
