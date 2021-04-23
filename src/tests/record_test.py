import unittest
import os
from record import ReplRecord

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

if __name__ == '__main__':
    unittest.main()