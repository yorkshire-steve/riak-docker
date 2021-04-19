#!/usr/bin/env python3.8
import struct
import os

RIAK_MAGIC_NUMBER = 53

class ReplRecord():

    def __init__(self, raw_data=None):
        self.raw_data = raw_data
        self.crc = 0
        self.is_delete = False
        self.valid = False
        self.compressed = False
        self.bucket = None
        self.key = None
        self.vectorClocks = None
        self.siblings_count = 0
        self.value = None
        self.last_modified = None
        self.vtag = None
        self.key_deleted = False
        self.metadata = []

        self.decode()
    
    def _isValid(self, offset):
        # record type and CRC32 of rest of data
        fs = '!2?I'
        (valid, delete, crc) = struct.unpack_from(fs, self.raw_data, offset=offset)

        if not valid:
            raise TypeError("Invalid record type")

        self.crc = crc
        self.is_delete = delete
        self.valid = valid

        return struct.calcsize(fs) + offset

    def _isCompressed(self, offset):
        # compression header (true means false I think?)
        fs = '!?'
        (compressed,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        self.compressed = compressed

        return struct.calcsize(fs) + offset

    def _getBucket(self, offset):
        # get bucket name
        fs = '!ii'
        # get bucket length (first 4 bytes are unused zeroes)
        (_, bucket_length) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        # get bucket name
        fs = '!' + str(bucket_length) + 's'
        (bucket,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        self.bucket = bucket

        return struct.calcsize(fs) + offset

    def _getKey(self, offset):
        # get key name
        fs = '!i'
        # get key length
        (key_length,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        # get key name
        fs = '!' + str(key_length) + 's'
        (key,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        self.key = key

        return struct.calcsize(fs) + offset

    def _getMagicNumber(self, offset):
        # magic number for Riak object and unused byte
        fs = '!bb'
        (magic,_) = struct.unpack_from(fs, self.raw_data, offset=offset)

        if magic != RIAK_MAGIC_NUMBER:
            raise TypeError("Invalid Riak object")

        return struct.calcsize(fs) + offset

    def _getVectorClocks(self, offset):
        # clocks length
        fs = '!i'
        (clock_length,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        # extract vector clocks
        fs = '!' + str(clock_length) + 's'
        (clocks,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        self.vectorClocks = clocks

        return struct.calcsize(fs) + offset

    def _getNumSiblings(self, offset):
        # number of siblings in record (usually 1)
        fs = '!i'
        (siblings_count,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        self.siblings_count = siblings_count

        return struct.calcsize(fs) + offset

    def _getValue(self, offset):
        fs = '!i'
        (value_length,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        # extract value
        fs = '!' + str(value_length) + 's'
        (value,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        self.value = value

        return struct.calcsize(fs) + offset

    def _extractMetaData(self, offset, metadata_length):
        offset_finish = offset + metadata_length

        fs = '!iiib'
        (lm_mega, lm_secs, lm_micro, vtag_len) = struct.unpack_from(fs,self.raw_data, offset=offset)
        self.last_modified = str(lm_mega) + str(lm_secs) + '.' + str(lm_micro)
        offset += struct.calcsize(fs)

        # extract vtag
        fs = '!' + str(vtag_len) + 's'
        (vtag,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        self.vtag = vtag
        offset += struct.calcsize(fs)

        # extract deleted (again?)
        fs = '!?'
        (deleted,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        self.key_deleted = deleted
        offset += struct.calcsize(fs)

        while offset < offset_finish:
            fs = '!i'
            (key_len,) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            fs = '!' + str(key_len) + 's'
            (key,) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            fs = '!i'
            (val_len,) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            fs = '!' + str(val_len) + 's'
            (val,) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            self.metadata.append({key:val})


    def _getMetaData(self,offset):
        fs = '!i'
        (metadata_length,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        self._extractMetaData(offset, metadata_length)

        return offset + metadata_length

    def decode(self):
        offset = self._isValid(0)

        offset = self._isCompressed(offset)
        offset = self._getBucket(offset)
        offset = self._getKey(offset)

        offset = self._getMagicNumber(offset)
        offset = self._getVectorClocks(offset)
        offset = self._getNumSiblings(offset)

        for _ in range(self.siblings_count):
            offset = self._getValue(offset)
            offset = self._getMetaData(offset)
        
        if offset != len(self.raw_data):
            raise TypeError("Did not fully decode record")


if __name__ == "__main__":
    with open(os.path.dirname(os.path.abspath(__file__)) + "/repl-test-files/test4",'rb') as f:
        data = f.read()
    
    rec = ReplRecord(data)

    print(rec.bucket, rec.key)
    print(rec.value)
    print(rec.last_modified, rec.vtag)
    print(rec.metadata)
