import unittest
import os
from record import ReplRecord, TooManySiblingsError

class TestReplRecord(unittest.TestCase):

    def test_normal_put(self):
        """
        Test a normal PUT record can be decoded
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        self.assertFalse(rec.empty)
        self.assertEqual(rec.crc, 3257020141)
        self.assertFalse(rec.is_delete)
        self.assertFalse(rec.compressed)
        self.assertIsNone(rec.bucket_type)
        self.assertEqual(rec.bucket, b'test')
        self.assertEqual(rec.key, b'test')
        self.assertEqual(rec.vector_clocks, b'g2wAAAACaAJtAAAACL8Aoe8A+zsmaAJhAm4FAHcc8tkOaAJtAAAADL8Aoe8A+0zuAAAAAWgCYQJuBQCtHfLZDmo=')
        self.assertEqual(rec.value, b'{"test":"data4"}')
        self.assertEqual(rec.last_modified, '1618846125.126554')
        self.assertEqual(rec.vtag, b'5kzmcxRpTdtQFl0IIuAbkF')
        self.assertIn({b'content-type': b'application/json'}, rec.metadata)

    def test_normal_put_vc_format_dict(self):
        """
        Test a normal PUT record can be decoded with vc_format=dict
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test",'rb') as f:
            data = f.read()

        rec = ReplRecord(data, vc_format='dict')

        self.assertFalse(rec.empty)
        self.assertEqual(rec.crc, 3257020141)
        self.assertFalse(rec.is_delete)
        self.assertFalse(rec.compressed)
        self.assertIsNone(rec.bucket_type)
        self.assertEqual(rec.bucket, b'test')
        self.assertEqual(rec.key, b'test')
        self.assertEqual(rec.vector_clocks, {'1090001219101612390251762380001': 2,
            '1090008191016123902515938': 2})
        self.assertEqual(rec.value, b'{"test":"data4"}')
        self.assertEqual(rec.last_modified, '1618846125.126554')
        self.assertEqual(rec.vtag, b'5kzmcxRpTdtQFl0IIuAbkF')
        self.assertIn({b'content-type': b'application/json'}, rec.metadata)

    def test_invalid_vc_format(self):
        with self.assertRaisesRegex(ValueError,'Invalid vector clock format invalid'):
            ReplRecord(b'', vc_format='invalid')

    def test_empty(self):
        """
        Test a replication record from empty queue can be decoded
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test2",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        self.assertTrue(rec.empty)

    def test_delete(self):
        """
        Test a DELETE record can be decoded
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test3",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        self.assertFalse(rec.empty)
        self.assertTrue(rec.is_delete)
        self.assertIsNotNone(rec.tomb_clock)
        self.assertTrue(rec.head_only)
        self.assertIsNone(rec.bucket_type)
        self.assertEqual(rec.bucket, b'test')
        self.assertEqual(rec.key, b'test')
        self.assertEqual(rec.tomb_clock, b'g2wAAAACaAJtAAAACL8Aoe8A+zsmaAJhAm4FAHcc8tkOaAJtAAAADL8Aoe8A+0zuAAAAAWgCYQNuBQDVKPLZDmo=')

    def test_invalid_checksum(self):
        """
        Test a record with invalid checksum raises exception
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test4",'rb') as f:
            data = f.read()

        with self.assertRaisesRegex(ValueError,'invalid checksum'):
            ReplRecord(data)

    def test_invalid_magic_number(self):
        """
        Test a record with invalid Riak object magic number raises exception
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test5",'rb') as f:
            data = f.read()

        with self.assertRaisesRegex(ValueError,'invalid riak object'):
            ReplRecord(data)

    def test_normal_put_compressed(self):
        """
        Test a normal PUT record with compression enabled can be decoded
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test6",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        self.assertFalse(rec.empty)
        self.assertEqual(rec.crc, 3675794333)
        self.assertFalse(rec.is_delete)
        self.assertTrue(rec.compressed)
        self.assertIsNone(rec.bucket_type)
        self.assertEqual(rec.bucket, b'test')
        self.assertEqual(rec.key, b'test')
        self.assertEqual(rec.value, b'{"test":"data4"}')
        self.assertEqual(rec.last_modified, '1618997295.800445')
        self.assertEqual(rec.vtag, b'e11V4Gy3vcpOBIVIuSmke')

    def test_normal_put_with_bucket_type(self):
        """
        Test a normal PUT with bucket type can be decoded
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test7",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        self.assertFalse(rec.empty)
        self.assertEqual(rec.crc, 3318186648)
        self.assertFalse(rec.is_delete)
        self.assertFalse(rec.compressed)
        self.assertEqual(rec.bucket_type, b'testType')
        self.assertEqual(rec.bucket, b'testBucket')
        self.assertEqual(rec.key, b'testKey')
        self.assertEqual(rec.value, b'{"test":"data"}')
        self.assertEqual(rec.last_modified, '1618999132.471832')
        self.assertEqual(rec.vtag, b'5NnOEeAXmRYucRkHl8KEXy')

    def test_too_long_record(self):
        """
        Test an invalid record (too long) raises exception
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test8",'rb') as f:
            data = f.read()

        with self.assertRaisesRegex(ValueError,'record too long'):
            ReplRecord(data)

    def test_too_many_siblings(self):
        """
        Test a record with more than 1 sibling raises exception
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test9",'rb') as f:
            data = f.read()

        with self.assertRaisesRegex(TooManySiblingsError, 'siblings=2 Too many siblings in record'):
            ReplRecord(data)

    def test_invalid_object_version(self):
        """
        Test a record with an invalid riak object version raises exception
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test10",'rb') as f:
            data = f.read()

        with self.assertRaisesRegex(ValueError,'only support v1 riak objects'):
            ReplRecord(data)

    def test_invalid_compression_flag(self):
        """
        Test a record with invalid compression flag raises exception
        """
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test11",'rb') as f:
            data = f.read()

        with self.assertRaisesRegex(ValueError,'invalid compression flag'):
            ReplRecord(data)


if __name__ == '__main__':
    unittest.main()