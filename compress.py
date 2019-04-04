# -*- coding: utf-8 -*-

import numpy as np

class Varbyte():
    def __init__(self, save_deltas=True, dtype=int, size=8):
        self.save_deltas = save_deltas
        self.max_size = size - 1
        self.max_digit = 2 ** self.max_size
        self.pack_type = 'uint' + str(size)
        self.unpack_type = dtype
        pass
    
    def pack(self, data):
        data = np.asarray(data, dtype=self.unpack_type)
        if self.save_deltas:
            data = data - np.concatenate((np.asarray([0]), data[:-1]))
        res = np.empty(0, dtype=self.pack_type)
        for i, num in enumerate(data):
            result = []
            tmp = num % self.max_digit + self.max_digit
            num //= self.max_digit
            while num > 0:
                result.insert(0, tmp)
                tmp = num % self.max_digit
                num /= self.max_digit
            result.insert(0, tmp)
            res = np.concatenate((res, result))
        return res
    
    def unpack(self, data):
        data = np.asarray(data, dtype=self.pack_type)
        res = np.empty(0, dtype=self.unpack_type)
        tmp = 0
        for num in data:
            tmp = tmp * self.max_digit + num
            if num >= self.max_digit:
                tmp -= self.max_digit
                res = np.concatenate((res, [tmp]))
                tmp = 0
        return np.cumsum(res) if self.save_deltas else res

class Simple9():
    def __init__(self, save_deltas=True):
        self.CODE_SIZE = 28
        self.HEADER_SIZE = 32 - self.CODE_SIZE
        self.SIZES = [28, 14, 9, 7, 5, 4, 3, 2, 1]
        self.NUMS = list((self.SIZES[8-i], (1 << self.SIZES[i]) - 1) for i in range(len(self.SIZES)))
        self.save_deltas = save_deltas
        pass
    
    def pack(self, data):
        data = np.asarray(data, dtype=int)
        if self.save_deltas:
            data = data - np.concatenate((np.asarray([0]), data[:-1]))
        res = np.empty(0, 'uint32')
        while data.shape[0] > 0:
            for i in xrange(1, len(self.NUMS)):
                if max(data[:self.NUMS[i][0]]) > self.NUMS[i][1]:
                    i -= 1
                    break
                if len(data) <= self.NUMS[i][0]:
                    break
            num = 0
            for j in xrange(min(self.NUMS[i][0], len(data))):
                num = (num << self.SIZES[i]) + data[0]
                data = data[1:]
            num += i << self.CODE_SIZE
            res = np.concatenate((res, [num]))
        return res
    
    def unpack(self, data):
        data = np.asarray(data, dtype='uint32')
        res = np.empty(0, int)
        for num in data:
            tmp = list()
            i = num >> self.CODE_SIZE
            num = num & ((1 << self.CODE_SIZE) - 1)
            while num  > 0:
                tmp.append(int(num & self.NUMS[i][1]))
                num >>= self.SIZES[i]
            for num in tmp[::-1]:
                res = np.concatenate((res, [num]))
        return np.cumsum(res) if self.save_deltas else res

class MurmurHash2():
    def __init__(self):
        pass
    
    def safe_mul(self, a, b):
        return (((a & 0xffff) * b) + ((((a >> 16) * b) & 0xffff) << 16)) % 2**32
    
    def __call__(self, data):
        _len = len(data)
        m = 0x5bd1e995
        h = _len
        while (_len >= 4):
            k  = ord(data[0])
            k |= ord(data[1]) << 8
            k |= ord(data[2]) << 16
            k |= ord(data[3]) << 24
            k = self.safe_mul(k, m)
            k ^= k >> 24
            k = self.safe_mul(k, m)
            h = self.safe_mul(h, m) ^ k
            data = data[4:]
            _len -= 4
        if _len == 3: h ^= ord(data[2]) << 16
        elif _len == 2: h ^= ord(data[1]) << 8
        elif _len == 1:
            h ^= ord(data[0])
            h = self.safe_mul(h, m)
        else: pass
        h ^= h >> 13
        h = self.safe_mul(h, m)
        h ^= h >> 15
        return h
