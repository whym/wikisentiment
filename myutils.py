#! /usr/bin/env python
# -*- coding: utf-8 -*-
# 

from datetime import datetime
from collections import namedtuple

wikilove_t = namedtuple('WikiLove', 'rev_id sender_id sender_name receiver_id receiver_name timestamp others')

def parse_wikidate(x):
    return datetime.strptime(str(x), '%Y%m%d%H%M%S')

def format_wikidate(x):
    return datetime.strftime(x, '%Y%m%d%H%M%S')
