# -*- coding: utf-8 -*-  

import sys
import os
import numpy as np
from struct import pack, unpack, calcsize
from indexer import Indexer

class Searcher():
    def __init__(self, indexer, querry_type='simple', tree='simple'):
        self.type = querry_type
        self.tree = tree
        self.indx = indexer
        pass
    
    def search(self, q):
        words = self.tokenize_query(q)
        doclists = []
        for word in words:
            termID = self.indx.hash(word)
            if termID not in self.indx.compressed_dict:
                doclists.append([])
            else:
                doclists.append(self.indx.compressor.unpack(self.indx.compressed_dict[termID]))
        res = []
        if len(doclists) == 1:
            res = doclists[0]
        else:
            for i, docID in enumerate(doclists[0]):
                flag = True
                for i in range(1, len(doclists)):
                    if docID not in doclists[i]: flag = False
                if flag: res.append(docID)
        return res

    def tokenize_query(self, q):
        if self.type == 'simple':
            return self.tokenize_simple_query(q)
        else:
            return ['']
        
    def tokenize_simple_query(self, q):
        return [i.replace(' ', '') for i in q.split('&')]
    
if __name__ == '__main__':
    indx = Indexer()
    indx.read()
    search = Searcher(indx)
    while True:
        words = sys.stdin.readline()
        if not words:
            break
        if words[-1] == '\n': words = words[:-1]
        print words
        res = search.search(words.decode('utf8').lower())
        print len(res)
        for i in res:
            print search.indx.urls[i]