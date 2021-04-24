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

    def _extract_value(self, format_string: str):
        try:
            (val,) = struct.unpack_from(format_string, self._raw_data, offset=self._offset)
        except struct.error as e:
            raise ValueError(e)
        self._offset += struct.calcsize(format_string)
        return val

    def _extract_bool(self):
        return self._extract_value('!?')

    def _extract_uint8(self):
        return self._extract_value('!B')

    def _extract_uint32(self):
        return self._extract_value('!I')

    def _extract_str(self, str_len: int):
        return self._extract_value('!' + str(str_len) + 's')

    def _extract_maybe_binary(self, value_length: int):
        is_binary = self._extract_bool()
        val = self._extract_str(value_length-1)

        if is_binary:
            return val
        else:
            try:
                return erlang.binary_to_term(val)
            except Exception as e:
                raise ValueError(e)

    def _is_empty(self):
        not_empty = self._extract_bool()

        if not_empty:
            self.empty = False

    def _is_delete(self):
        self.is_delete = self._extract_bool()

    def _is_valid(self):
        self.crc = self._extract_uint32()

        if self.crc != zlib.crc32(self._raw_data[self._offset:]):
            raise ValueError("invalid checksum")

    def _is_compressed(self):
        compressed = self._extract_uint8()

        if compressed == 16:
            self.compressed = False
        elif compressed == 24:
            self.compressed = True
        else:
            raise ValueError("invalid compression flag")

    def _decompress(self):
        self._raw_data = zlib.decompress(self._raw_data[self._offset:])
        self._offset = 0

    def _get_bucket_type(self):
        type_length = self._extract_uint32()

        if type_length != 0:
            self.bucket_type = self._extract_str(type_length)

    def _get_bucket(self):
        bucket_length = self._extract_uint32()

        if bucket_length != 0:
            self.bucket = self._extract_str(bucket_length)

    def _get_key(self):
        key_length = self._extract_uint32()

        if key_length != 0:
            self.key = self._extract_str(key_length)

    def _get_magic_number(self):
        magic = self._extract_uint8()

        if magic != RIAK_MAGIC_NUMBER:
            raise ValueError("invalid riak object")

        obj_version = self._extract_uint8()

        if obj_version != 1:
            raise ValueError("only support v1 riak objects")

    def _get_vector_clocks(self):
        clock_length = self._extract_uint32()

        if clock_length != 0:
            self.vector_clocks = base64.b64encode(self._extract_str(clock_length))

    def _get_num_siblings(self):
        self.siblings_count = self._extract_uint32()

        # TODO: Add support for multiple siblings
        if self.siblings_count != 1:
            raise TooManySiblingsError(self.siblings_count)

    def _get_value(self):
        value_length = self._extract_uint32()

        if value_length == 1:
            self.head_only = True

        self.value = self._extract_maybe_binary(value_length)

    def _get_meta_data(self):
        metadata_length = self._extract_uint32()
        offset_finish = self._offset + metadata_length

        lm_mega = self._extract_uint32()
        lm_secs = self._extract_uint32()
        lm_micro = self._extract_uint32()
        self.last_modified = str(lm_mega) + str(lm_secs) + '.' + str(lm_micro)

        vtag_len = self._extract_uint8()
        self.vtag = self._extract_str(vtag_len)

        self.key_deleted = self._extract_bool()

        # extract metadata key/value pairs
        while self._offset < offset_finish:
            key_len = self._extract_uint32()
            key = self._extract_maybe_binary(key_len)

            val_len = self._extract_uint32()
            val = self._extract_maybe_binary(val_len)

            self.metadata.append({key:val})

    def _get_tomb_clock(self):
        tomb_clock_len = self._extract_uint32()

        if tomb_clock_len != 0:
            self.tomb_clock = base64.b64encode(self._extract_str(tomb_clock_len))

    def decode(self):
        self._is_empty()

        if self.empty:
            return

        self._is_delete()

        if self.is_delete:
            self._get_tomb_clock()

        self._is_valid()

        self._is_compressed()
        self._get_bucket_type()
        self._get_bucket()
        self._get_key()

        if self.compressed:
            self._decompress()

        self._get_magic_number()
        self._get_vector_clocks()
        self._get_num_siblings()

        for _ in range(self.siblings_count):
            self._get_value()
            self._get_meta_data()

        if self._offset != len(self._raw_data):
            raise ValueError("record too long")

        del self._raw_data
