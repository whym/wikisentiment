#! /bin/zsh
/home/yusuke/projects/wikisentiment/wikilove_categorized.py -d enwiki_p -s `TZ=UTC+24 date +%Y%m%d%H%M%S` -H 127.0.0.1:3307 --mongo-hosts localhost -l 300 -o ~/public_html/wikilove/latest.html
