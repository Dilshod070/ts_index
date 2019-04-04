# -*- coding: utf-8 -*-  

import sys
import os
import numpy as np
import argparse
import document_pb2
import gzip
from struct import pack, unpack
from hashlib import md5

import doc2words
import docreader
from compress import Varbyte, Simple9, MurmurHash2

class Indexer():
    """
    Makes, Compresses, and Saves dict
    """
    def __init__(self, compressor='varbyte', hash_='md5/2', coding='utf8'):
        """
        compressors: ['varbyte', 'simple9']
        hash_: ['mmh2', 'md5/2'] (коллизии при mmh2!!!)
        coding: any (tested only with utf8)
        """
        self.dict = {}
        self.compressed_dict = None
        self.urls = {}
           
        self.compressor_type = compressor
        if compressor == 'varbyte': self.compressor = Varbyte()
        elif compressor == 'simple9': self.compressor = Simple9()
        else: raise RuntimeError('Compressor: {} not found'.format(compressor))
            
        self.hash_type = hash_
        if hash_ == 'mmh2': self.hash = MurmurHash2()
        elif hash_ == 'md5/2': self.hash = self.half_md5
        else: raise RuntimeError('Hash: {} not found'.format(hash_))
            
        self.coding = coding
    
    def half_md5(self, data):
        data = unicode(data).encode(self.coding)
        return int(md5(data).hexdigest()[:16], 16)


    def make_dict(self, doc_reader):
        for docID, doc in enumerate(doc_reader):
            url = doc.url # не буду делать DocID <=> doc.url
            body = doc.body # Там нет ни в одном документе тела
            text = doc.text
            words = doc2words.extract_words(text)
            self.urls[docID] = url
#             if docID == 21: 
#                 print doc.url
#                 print "BODY\n", doc.body
#                 print "TEXT\n", doc.text
#                 for word in words:
#                     if self.hash(word) == self.hash(u'сша'): print word
            for word in words:
                termID = self.hash(word)
                if termID in self.dict:
                    if docID not in self.dict[termID]:
                        self.dict[termID] = np.concatenate((self.dict[termID], [docID]))
                else:
                    self.dict[termID] = np.asarray([docID])
#         print self.dict[self.hash(u'сша')]
    
    def compress(self):
        if self.compressed_dict is not None:
            print >> sys.stderr, 'Dictionary was already compressed'
        self.compressed_dict = {}
        for termID in self.dict:
            self.compressed_dict[termID] = self.compressor.pack(self.dict[termID])
        # Удалить словарь, чтобы не занимал память?
        self.dict = {}
        
    def save(self, OUTPUT_FILE='index.dat', OUTPUT_FILE2='urls.dat'): # Работает только с hash_size <= uint64
        if self.compressed_dict is None:
            raise RuntimeError('Compress dictionaty before saving')
        output = open(OUTPUT_FILE, 'wb')
        output2 = open(OUTPUT_FILE2, 'wb')
        output.write(self.compressor_type + '\n')
        output.write(self.hash_type + '\n')
        for termID in self.compressed_dict:
            array = self.compressed_dict[termID]
            if self.compressor_type == 'varbyte':
                output.write(pack('QQ%sB' % array.shape, termID, array.shape[0], *array))
            elif self.compressor_type == 'simple9':
                output.write(pack('QQ%sI' % array.shape, termID, array.shape[0], *array))
            else: raise RuntimeError('Unknown compress type: {}'.format(self.compressor_type))
        for docID in self.urls:
            url = self.urls[docID]
            output2.write(pack('Q', docID))
            output2.write((url + '\n').encode('utf8'))
        output.close()
        output2.close()
        
    def read(self, INPUT_FILE='index.dat', INPUT_FILE2='urls.dat'): # Работает только с hash_size <= uint64
        if self.compressed_dict is not None:
            print >> sys.stderr, 'Dictionary was already read'
        self.compressed_dict = {}
        self.urls = {}
        input_file = open(INPUT_FILE, 'rb')
        input_file2 = open(INPUT_FILE2, 'rb')
        self.compressor_type = input_file.readline()[:-1]
        self.hash_type = input_file.readline()[:-1]
        if self.compressor_type == 'varbyte': self.compressor = Varbyte()
        elif self.compressor_type == 'simple9': self.compressor = Simple9()
        else: raise RuntimeError('Compressor: {} not found'.format(self.compressor_type))
        while True:
            buf = input_file.read(16)
            if len(buf) != 16:
                if len(buf) == 0: break
                else: raise RuntimeError('unexpected EOF')
            termID, array_size = unpack('QQ', buf)
            if self.compressor_type == 'varbyte':
                buf = input_file.read(array_size)
                self.compressed_dict[termID] = np.asarray(unpack('%sB' % array_size, buf), dtype='uint8')
            elif self.compressor_type == 'simple9':
                buf = input_file.read(4 * array_size)
                self.compressed_dict[termID] = np.asarray(unpack('%sI' % array_size, buf), dtype='uint32')
            else: raise RuntimeError('Unknown compress type: {}'.format(self.compressor_type))
                
        while True:
            buf = input_file2.read(8)
            if len(buf) != 8:
                if len(buf) == 0: break
                else: raise RuntimeError('unexpected EOF')
            docID = unpack('Q', buf)[0]
            url = input_file2.readline()
            self.urls[docID] = url[:-1]
        input_file.close()
        input_file2.close()
        
def parse_command_line():
    parser = argparse.ArgumentParser(description='compressed documents reader')
    parser.add_argument('args', nargs='+', help='Input files (.gz or plain) to process')
    return parser.parse_args()
        
if __name__ == '__main__':
    args = parse_command_line().args
    compressor = args.pop(0)
   
    reader = docreader.DocumentStreamReader(args)
    
    Idxr = Indexer(compressor=compressor)
    Idxr.make_dict(reader)
    Idxr.compress()
    Idxr.save()
    
#     Idxr2 = Indexer()
#     Idxr2.read()
    
#     try:
#         for termID in Idxr2.compressed_dict:
#             if termID not in Idxr.compressed_dict:
#                 raise RuntimeError('Wrong key after save')
#             if not Idxr2.compressed_dict[termID].all() == Idxr.compressed_dict[termID].all():
#                 raise RuntimeError('Wrong value after save')
#         print 'OK'
#     except RuntimeError as e:
#         print e
#         print 'NOT OK'