#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sys
import pymongo
import argparse
import urllib2
import time
import murmur
import liblinear.linear
import liblinear.linearutil
import ast
import tempfile
from collections import namedtuple

from twisted.internet import reactor
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from xml.dom import minidom

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--prediction', metavar='N',
                        dest='pred', type=lambda x: int(x)-1, default=2,
                        help='the column number for predictions')
    parser.add_argument('-c', '--coded', metavar='N',
                        dest='code', type=lambda x: int(x)-1, default=3,
                        help='the column number for codes')
    parser.add_argument('-l', '--label', metavar='N',
                        dest='label', type=lambda x: int(x)-1, default=0,
                        help='the column number for labels')
    parser.add_argument('-I', '--ignore', metavar='STR',
                        dest='ignore', type=str, default='Unknown',
                        help='the code value should not be counted')
    parser.add_argument('-T', '--true', metavar='STR',
                        dest='positive', type=str, default='True',
                        help='the code value should not be counted')
    parser.add_argument('-D', '--delimiter', metavar='CHARACTER',
                        dest='delimiter', type=lambda x: ast.literal_eval('"'+x+'"'), default='\t',
                        help='')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    parser.add_argument('input', nargs='+', type=lambda x: open(x))
    
    options = parser.parse_args()

    pn_tuple = namedtuple('pn', 'p n')

    # load raw table of coded examples
    csv.field_size_limit(1000000000)
    table = []
    for f in options.input:
        t = list(csv.reader(f, delimiter=options.delimiter))
        table += t[1:]
    pns = {}
    for cols in table:
        lab,pred,code = [cols[x] for x in [options.label, options.pred, options.code]]
        pn = pns.setdefault(lab, pn_tuple({True: 0, False: 0},
                                          {True: 0, False: 0}))
        if code == options.ignore:
            None
        else:
            ok = (pred == code)
            if pred == options.positive:
                pn.p[ok] += 1
            else:
                pn.n[ok] += 1
    for (lab,pn) in sorted(pns.items(), key=lambda x: x[0]):
        print lab
        numcorrect = pn.p[True] + pn.n[True]
        numwrong   = pn.p[False] + pn.n[False]
        print ' accuracy  = %f (%d/%d)' % (float(numcorrect) / (numcorrect + numwrong) if numcorrect + numwrong > 0 else float('nan'),
                                           numcorrect,
                                           (numcorrect + numwrong))
        prec = float(pn.p[True]) / sum(pn.p.values()) if sum(pn.p.values()) != 0 else float('nan')
        reca = float(pn.p[True]) / (pn.p[True] + pn.n[False]) if pn.p[True] + pn.n[False] != 0 else float('nan')
        print ' precision = %f' % prec
        print ' recall    = %f' % reca
        print ' fmeasure  = %f' % (1.0 / (0.5/prec + 0.5/reca) if prec != 0 and reca != 0 else float('nan'))
        print '', pn, (pn.p[True] + pn.p[False] + pn.n[True] + pn.n[False])
