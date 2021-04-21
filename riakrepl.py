import struct
import zlib
import base64
import erlang

RIAK_MAGIC_NUMBER = 53

class ReplRecord():

    def __init__(self, raw_data=None):
        self.raw_data = raw_data
        self.empty = True
        self.crc = 0
        self.is_delete = False
        self.tomb_clock = None
        self.compressed = False
        self.bucket_type = None
        self.bucket = None
        self.key = None
        self.vector_clocks = None
        self.siblings_count = 0
        self.head_only = False
        self.value = None
        self.last_modified = None
        self.vtag = None
        self.key_deleted = False
        self.metadata = []

        self.decode()

    def _isEmpty(self):
        # check if repl record is empty
        fs = '!?'
        (not_empty,) = struct.unpack_from(fs, self.raw_data, offset=0)

        if not_empty:
            self.empty = False

        return struct.calcsize(fs)

    def _isDelete(self, offset):
        # is this a delete record
        fs = '!?'
        (delete,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)
        self.is_delete = delete

        return offset

    def _isValid(self, offset):
        # record type and CRC32 of rest of data
        fs = '!I'
        (crc,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        self.crc = crc

        if crc != zlib.crc32(self.raw_data[offset:]):
            raise ValueError("invalid checksum")

        return offset

    def _isCompressed(self, offset):
        # compression header
        fs = '!B'
        (compressed,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        if compressed == 16:
            self.compressed = False
        elif compressed == 24:
            self.compressed = True
        else:
            raise ValueError("invalid compression flag")

        return offset

    def _decompress(self, offset):
        self.raw_data = zlib.decompress(self.raw_data[offset:])
        offset = 0
        return offset

    def _getBucketType(self, offset):
        fs = '!I'
        # get type length
        (type_length,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        if type_length != 0:
            # get type name
            fs = '!' + str(type_length) + 's'
            (bucket_type,) = struct.unpack_from(fs, self.raw_data, offset=offset)
            offset += struct.calcsize(fs)
            self.bucket_type = bucket_type

        return offset

    def _getBucket(self, offset):
        # get bucket name
        fs = '!I'
        # get bucket length
        (bucket_length,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        if bucket_length != 0:
            # get bucket name
            fs = '!' + str(bucket_length) + 's'
            (bucket,) = struct.unpack_from(fs, self.raw_data, offset=offset)
            offset += struct.calcsize(fs)
            self.bucket = bucket

        return offset

    def _getKey(self, offset):
        # get key name
        fs = '!I'
        # get key length
        (key_length,) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        if key_length != 0:
            # get key name
            fs = '!' + str(key_length) + 's'
            (key,) = struct.unpack_from(fs, self.raw_data, offset=offset)
            offset += struct.calcsize(fs)
            self.key = key

        return offset

    def _getMagicNumber(self, offset):
        # magic number for Riak object and unused byte
        fs = '!BB'
        (magic, obj_version) = struct.unpack_from(fs, self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        if magic != RIAK_MAGIC_NUMBER:
            raise ValueError("invalid riak object")

        if obj_version != 1:
            raise ValueError("only support v1 riak objects")

        return offset

    def _getVectorClocks(self, offset):
        # clocks length
        fs = '!I'
        (clock_length,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        # extract vector clocks
        fs = '!' + str(clock_length) + 's'
        (clocks,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)
        self.vector_clocks = base64.b64encode(clocks)
        #self.vector_clocks = erlang.binary_to_term(clocks)

        return offset

    def _getNumSiblings(self, offset):
        # number of siblings in record (usually 1)
        fs = '!I'
        (siblings_count,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)
        self.siblings_count = siblings_count

        # TODO: Add support for multiple siblings
        if siblings_count != 1:
            raise ValueError("record has multiple siblings")

        return offset

    def _getValue(self, offset):
        fs = '!I'
        (value_length,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        if value_length == 1:
            self.head_only = True

        # extract value
        fs = '!?' + str(value_length-1) + 's'
        (is_binary,value) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        if is_binary:
            self.value = value
        else:
            self.value = erlang.binary_to_term(value)

        return offset

    def _extractMetaData(self, offset, metadata_length):
        offset_finish = offset + metadata_length

        fs = '!IIIB'
        (lm_mega, lm_secs, lm_micro, vtag_len) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)
        self.last_modified = str(lm_mega) + str(lm_secs) + '.' + str(lm_micro)

        # extract vtag
        fs = '!' + str(vtag_len) + 's'
        (vtag,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)
        self.vtag = vtag

        # extract deleted (again?)
        fs = '!?'
        (deleted,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)
        self.key_deleted = deleted

        while offset < offset_finish:
            fs = '!I'
            (key_len,) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            fs = '!?' + str(key_len-1) + 's'
            (is_binary, key) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            if not is_binary:
                key = erlang.binary_to_term(key)

            fs = '!I'
            (val_len,) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            fs = '!?' + str(val_len-1) + 's'
            (is_binary, val) = struct.unpack_from(fs,self.raw_data, offset=offset)
            offset += struct.calcsize(fs)

            if not is_binary:
                val = erlang.binary_to_term(val)

            # first byte of key and val are effectively meaningless
            self.metadata.append({key:val})


    def _getMetaData(self, offset):
        fs = '!I'
        (metadata_length,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        self._extractMetaData(offset, metadata_length)

        return offset + metadata_length

    def _getTombClock(self, offset):
        fs = '!I'
        (tomb_clock_len,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)

        # extract tomb clock
        fs = '!' + str(tomb_clock_len) + 's'
        (tomb_clock,) = struct.unpack_from(fs,self.raw_data, offset=offset)
        offset += struct.calcsize(fs)
        self.tomb_clock = base64.b64encode(tomb_clock)

        return offset

    def decode(self):
        offset = self._isEmpty()

        if self.empty:
            return

        offset = self._isDelete(offset)

        if self.is_delete:
            offset = self._getTombClock(offset)

        offset = self._isValid(offset)

        offset = self._isCompressed(offset)
        offset = self._getBucketType(offset)
        offset = self._getBucket(offset)
        offset = self._getKey(offset)

        if self.compressed:
            offset = self._decompress(offset)

        offset = self._getMagicNumber(offset)
        offset = self._getVectorClocks(offset)
        offset = self._getNumSiblings(offset)

        for _ in range(self.siblings_count):
            offset = self._getValue(offset)
            offset = self._getMetaData(offset)

        if offset != len(self.raw_data):
            raise ValueError("record too long")
