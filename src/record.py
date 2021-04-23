import struct
import zlib
import base64
import erlang

RIAK_MAGIC_NUMBER = 53

class TooManySiblingsError(Exception):
    """Exception raised for too many siblings in repl record.

    Attributes:
        num_sublings -- number of siblings in record
        message -- explanation of the error
    """

    def __init__(self, num_sublings, message="Too many siblings in record"):
        self.num_sublings = num_sublings
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'siblings={self.num_sublings} {self.message}'

class ReplRecord():

    def __init__(self, raw_data=None):
        self._raw_data = raw_data

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

        self._offset = 0

        self.decode()

    def _extractValue(self, format_string):
        try:
            (val,) = struct.unpack_from(format_string, self._raw_data, offset=self._offset)
        except struct.error as e:
            raise ValueError(e)
        self._offset += struct.calcsize(format_string)
        return val

    def _extractBool(self):
        return self._extractValue('!?')

    def _extractUINT8(self):
        return self._extractValue('!B')

    def _extractUINT32(self):
        return self._extractValue('!I')

    def _extractStr(self, str_len):
        return self._extractValue('!' + str(str_len) + 's')

    def _extractMaybeBinary(self, value_length):
        is_binary = self._extractBool()
        val = self._extractStr(value_length-1)

        if is_binary:
            return val
        else:
            try:
                return erlang.binary_to_term(val)
            except Exception as e:
                raise ValueError(e)

    def _isEmpty(self):
        not_empty = self._extractBool()

        if not_empty:
            self.empty = False

    def _isDelete(self):
        self.is_delete = self._extractBool()

    def _isValid(self):
        self.crc = self._extractUINT32()

        if self.crc != zlib.crc32(self._raw_data[self._offset:]):
            raise ValueError("invalid checksum")

    def _isCompressed(self):
        compressed = self._extractUINT8()

        if compressed == 16:
            self.compressed = False
        elif compressed == 24:
            self.compressed = True
        else:
            raise ValueError("invalid compression flag")

    def _decompress(self):
        self._raw_data = zlib.decompress(self._raw_data[self._offset:])
        self._offset = 0

    def _getBucketType(self):
        type_length = self._extractUINT32()

        if type_length != 0:
            self.bucket_type = self._extractStr(type_length)

    def _getBucket(self):
        bucket_length = self._extractUINT32()

        if bucket_length != 0:
            self.bucket = self._extractStr(bucket_length)

    def _getKey(self):
        key_length = self._extractUINT32()

        if key_length != 0:
            self.key = self._extractStr(key_length)

    def _getMagicNumber(self):
        magic = self._extractUINT8()

        if magic != RIAK_MAGIC_NUMBER:
            raise ValueError("invalid riak object")

        obj_version = self._extractUINT8()

        if obj_version != 1:
            raise ValueError("only support v1 riak objects")

    def _getVectorClocks(self):
        clock_length = self._extractUINT32()

        if clock_length != 0:
            self.vector_clocks = base64.b64encode(self._extractStr(clock_length))

    def _getNumSiblings(self):
        self.siblings_count = self._extractUINT32()

        # TODO: Add support for multiple siblings
        if self.siblings_count != 1:
            raise TooManySiblingsError(self.siblings_count)

    def _getValue(self):
        value_length = self._extractUINT32()

        if value_length == 1:
            self.head_only = True

        self.value = self._extractMaybeBinary(value_length)

    def _getMetaData(self):
        metadata_length = self._extractUINT32()
        offset_finish = self._offset + metadata_length

        lm_mega = self._extractUINT32()
        lm_secs = self._extractUINT32()
        lm_micro = self._extractUINT32()
        self.last_modified = str(lm_mega) + str(lm_secs) + '.' + str(lm_micro)

        vtag_len = self._extractUINT8()
        self.vtag = self._extractStr(vtag_len)

        self.key_deleted = self._extractBool()

        # extract metadata key/value pairs
        while self._offset < offset_finish:
            key_len = self._extractUINT32()
            key = self._extractMaybeBinary(key_len)

            val_len = self._extractUINT32()
            val = self._extractMaybeBinary(val_len)

            self.metadata.append({key:val})

    def _getTombClock(self):
        tomb_clock_len = self._extractUINT32()

        if tomb_clock_len != 0:
            self.tomb_clock = base64.b64encode(self._extractStr(tomb_clock_len))

    def decode(self):
        self._isEmpty()

        if self.empty:
            return

        self._isDelete()

        if self.is_delete:
            self._getTombClock()

        self._isValid()

        self._isCompressed()
        self._getBucketType()
        self._getBucket()
        self._getKey()

        if self.compressed:
            self._decompress()

        self._getMagicNumber()
        self._getVectorClocks()
        self._getNumSiblings()

        for _ in range(self.siblings_count):
            self._getValue()
            self._getMetaData()

        if self._offset != len(self._raw_data):
            raise ValueError("record too long")

        del self._raw_data
