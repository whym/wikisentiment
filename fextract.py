import csv
import re
import nltk

class WikiSyntaxExtractor:
    def __init__(self):
        self.patterns = {}
        self.patterns['WP_COLON'] = re.compile('\[\[(wp|WP|wikipedia|Wikipedia):')
        self.patterns['HELP_COLON'] = re.compile('\[\[(help|Help):')
        self.patterns['TMPL'] = re.compile('\{\{')
        self.patterns['TMPLONLY'] = re.compile('^\{\{.*\}\}$')
        self.patterns['TMPLLINK'] = re.compile('[tT]emplate:')
        self.patterns['VANDAL'] = re.compile('[vV]andal')
        self.patterns['BLOCK'] = re.compile('[bB]lock')
        self.patterns['WIKIPROJECT'] = re.compile('[wW]iki( |)[pP]roject')
        self.patterns['REDIRECT'] = re.compile('\#(redirect|REDIRECT)')
        self.patterns['DELETION'] = re.compile('\[\[Wikipedia\:(Articles for deletion|Proposed deletion)\|')
    def extract(self, entry):
        ret = {}
        for (pname,pat) in self.patterns.items():
            if pat.search(entry['entry']['content']):
                ret[pname] = 1.0
        return ret
    def name(self):
        return 'WikiSyntaxExtractor'

class NgramExtractor:
    def __init__(self, n=2):
        self.n = n
        self.wordsegment = re.compile('[ \{\}\n\\(\)\'>]') # use NLTK segmenter
    def extract(self, entry):
        ret = {}
        words = self.wordsegment.split(entry['entry']['content'])
        for ng in nltk.ngrams(words, self.n):
            ret['_'.join(ng)] = True
            if len(ret) == 100:
                break
        return ret
    def name(self):
        return 'NgramExtractor|%(n)s' % self.__dict__

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
        # TODO: proper word segmentation (ex. using NLTK?)
        # TODO: try pos tatgging and sense disambiguation
        #print entry['entry']
        words = self.wordsegment.split(entry['entry']['content'])
        words = filter(lambda x: len(x) > 0, words)
        words = map(lambda x: re.sub('[\?\!\.,;:\-"\']$', '', x), words)
        words = map(lambda x: self.lemmatizer.lemmatize(x), words)
        ret = {}
        for w in words:
            if self.avg_scores.has_key(w):
                if abs(self.avg_scores[w]) > self.threshold:
                    ret[w] = self.avg_scores[w]
        return ret
    def name(self):
        return 'SentiWordNetExtractor|%(threshold)f' % self.__dict__

if __name__ == '__main__':
    # some examples
    for fx in [SentiWordNetExtractor('SentiWordNet_3.0.0_20100908.txt'), NgramExtractor(2), WikiSyntaxExtractor()]:
        print fx.name()
        print fx.extract({"entry" : { "content" : "{{welcome}}\n[[User:Jwrosenzweig|Jwrosenzweig]] 00:37, 1 Feb 2005 (UTC)\nP.S. I've reformatted [[Marrowstone, Washington]] a little so that the external link to Fort Flagler is in the external links section, and so that [[Fort Flagler]] links to the empty article on the fort (maybe you'd take a shot at writing it?).  Give it a look if you have time.  Thanks for your contributions: they're very appreciated!", "receiver" : "Vishakha", "sender" : "Jwrosenzweig", "rev_id" : 17231315, "title" : "Vishakha" } })
        print fx.extract({"entry" : { "content" : "Hi, ElfineM, welcome to Wikipedia. I hope you like the place and choose to [[Wikipedia:Wikipedians|stay]]... Check out [[Template:Welcome]] for some good links to help you get started, if you need to.\n\nJust a quick point, if you want to comment on an article, usually the most recent talk goes at the bottom of the page. I've done this for you at [[Talk:Feminism]].\n\nIf you've any more questions you have, don't hesiste to ask me at my [[User talk:Dysprosia|talk page]], or on the [[Wikipedia:Village pump]].\n\nHave fun! [[User:Dysprosia|Dysprosia]] 05:10, 5 Feb 2005 (UTC)", "receiver" : "Lincspoacher", "sender" : "Dysprosia", "rev_id" : 10192272, "title" : "Lincspoacher" } })
