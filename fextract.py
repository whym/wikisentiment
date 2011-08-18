import csv
import re
import nltk
import sys

def extract_contents(ent):
    for (name, ext) in {'added':   lambda x: x['entry']['content']['added'],
                        'removed': lambda x: x['entry']['content']['removed'],
                        'comment': lambda x: x['entry']['comment'],}.items():
        try:
            yield(name, ext(ent))
        except KeyError:
            print >>sys.stderr, 'empty', name, ent

def longest_subsequence(x, y):
    l = len(y)
    mstart = 0
    mcont = 0
    start = 0
    cont = 0
    for (i,v) in enumerate(x):
        if i >= l:
            break
        if v == y[i]:
            if cont == 0:
                start = i
            cont += 1
        else:
            cont = 0
        if cont >= mcont:
            mstart = start
            mcont  = cont
    return (mstart, mstart + mcont)

def uncapitalize(str):
    (start,end) = longest_subsequence(str, str.upper())
    ret = str
    if end - start > 50:
        ret = str[0:start] + str[start:end].lower() + str[end:]
    return ret

class WikiPatternExtractor:
    def __init__(self, file='patterns.txt'):
        self.patterns = {}
        self.patterns_expanded = {}
        for line in open(file):
            cstart = line.find('#')
            if cstart >= 0:
                cc = line.find('\\#')
                if cc + 1 != cstart:
                    line = line[0:cstart]

            line = line.strip()
            if len(line) == 0:
                continue
                
            a = line.split('\t')
            expand = ''
            if len(a) == 3:
                expand = a.pop()
            name,patt = a
            if expand.find('expand') >= 0:
                self.patterns_expanded[name] = re.compile(patt)
            if expand.find('binary') >= 0 or expand == '':
                self.patterns[name] = re.compile(patt)
    def extract(self, entry):
        ret = {}
        for (cname, contents) in extract_contents(entry):
            for (pname,pat) in self.patterns.items():
                if pat.search(' '.join(contents)):
                    ret['_'.join([pname,cname])] = True
            for (pname,pat) in self.patterns_expanded.items():
                for m in pat.finditer(' '.join(contents)):
                    ret['_'.join([pname,cname]+list(m.groups()))] = True
        return ret
    def name(self):
        return 'WikiPatternExtractor'

class NgramExtractor:
    def __init__(self, n=2, lowercase=False):
        self.n = n
        self.wordsegment = re.compile('[ \{\}\n\\(\)\'>]') # use NLTK segmenter
        self.lowercase = lowercase
    def extract(self, entry):
        ret = {}
        for (cname,contents) in extract_contents(entry):
            s = ' '.join(contents)
            if self.lowercase:
                s = uncapitalize(s)
            words = self.wordsegment.split(s)
            for ng in nltk.ngrams(words, self.n):
                ret['_'.join([cname]+list(ng))] = True
                if len(ret) == 100:
                    break
        return ret
    def name(self):
        return 'NgramExtractor|%(n)s|lc=%(lowercase)s' % self.__dict__

class MediaWikiExtractor:
    None

class SentiWordNetExtractor:
    def __init__(self, file, threshold=0.1):
        table = list(csv.reader(filter(lambda x: x[0] != '#', open(file)), delimiter='\t'))
        self.lemmatizer = nltk.stem.WordNetStemmer()
        self.threshold = 0.1
        self.words = {}
        for cols in table:
            if len(cols[0]) != 1:
                continue
            synsets = cols[4].split(' ')
            pscore = float(cols[2])
            nscore = float(cols[3])
            if pscore == 0 and nscore == 0:
                continue
            for ss in synsets:
                (w,n) = ss.split('#')
                self.words.setdefault(w, []).append((pscore,nscore))
        self.wordsegment = re.compile('[ \{\}\n\\(\)\'>]')

        self.avg_scores = {}
        for (w, scores) in self.words.items():
            a = 0.0
            for (p,n) in scores:
                a += p - n
            self.avg_scores[w] = a/len(scores)

    def extract(self, entry):
        # TODO: try pos tatgging and sense disambiguation
        ret = {}
        for (cname,contents) in extract_contents(entry):
            words = self.wordsegment.split(uncapitalize(' '.join(contents)))
            words = filter(lambda x: len(x) > 0, words)
            words = map(lambda x: re.sub('[\?\!\.,;:\-"\']$', '', x), words)
            words = map(lambda x: self.lemmatizer.lemmatize(x), words)
            for w in words:
                if self.avg_scores.has_key(w):
                    if abs(self.avg_scores[w]) > self.threshold:
                        ret['_'.join([cname,w])] = self.avg_scores[w]
        return ret
    def name(self):
        return 'SentiWordNetExtractor|%(threshold)f' % self.__dict__

if __name__ == '__main__':
    # some examples
    for fx in [SentiWordNetExtractor('SentiWordNet_3.0.0_20100908.txt'), NgramExtractor(2), NgramExtractor(2, lowercase=True), WikiPatternExtractor()]:
        print fx.name()
        print fx.extract({"entry" : { "content" : {'added': ["{{welcome}}\n[[User:Jwrosenzweig|Jwrosenzweig]] 00:37, 1 Feb 2005 (UTC)\nP.S. I've reformatted [[Marrowstone, Washington]] a little so that the external link to Fort Flagler is in the external links section, and so that [[Fort Flagler]] links to the empty article on the fort (maybe you'd take a shot at writing it?).  Give it a look if you have time.  Thanks for your contributions: they're very appreciated!"], 'removed':[]}, "receiver" : "Vishakha", "sender" : "Jwrosenzweig", "id": {"rev_id" : 17231315}, "title" : "Vishakha", 'comment': [] } })
        print fx.extract({"entry" : { "content" : {'added': ["Hi, ElfineM, welcome to Wikipedia. I hope you like the place and choose to [[Wikipedia:Wikipedians|stay]]... Check out [[Template:Welcome]] for some good links to help you get started, if you need to.\n\nJust a quick point, if you want to comment on an article, usually the most recent talk goes at the bottom of the page. I've done this for you at [[Talk:Feminism]].\n\nIf you've any more questions you have, don't hesiste to ask me at my [[User talk:Dysprosia|talk page]], or on the [[Wikipedia:Village pump]].\n\nHave fun! [[User:Dysprosia|Dysprosia]] 05:10, 5 Feb 2005 (UTC)"], 'removed':[]}, "receiver" : "Lincspoacher", "sender" : "Dysprosia", "rev_id" : 10192272, "title" : "Lincspoacher", 'comment': [] } })
        print fx.extract({"entry" : { "content" : {'added': ["DO NOT POST THE FINAL AIRING OF WBHS AGAIN...WBHS WILL STAY ON AS LONG AS I AM A STUDENT AT BHS!!!! You are a sick human being to post \"I support Insurgency\" on your page...21:22, 8 February 2007 (UTC)Kgregory21:22, 8 February 2007 (UTC)"], 'removed':[]}, "receiver" : "Trillionaire", "sender" : "Kgregory", "id": {"rev_id" : 10192272}, "title" : "Trillionaire", 'comment': [] } })
