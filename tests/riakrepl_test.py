import unittest
import os
from riakrepl import ReplRecord

class TestReplRecord(unittest.TestCase):

    def test_normal_put(self):
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        # self.vectorClocks = None
        # self.key_deleted = False
        # self.metadata = []

        self.assertFalse(rec.empty)
        self.assertEqual(rec.crc, 3257020141)
        self.assertFalse(rec.is_delete)
        self.assertFalse(rec.compressed)
        self.assertEqual(rec.bucket, b'test')
        self.assertEqual(rec.key, b'test')
        self.assertEqual(rec.value, b'{"test":"data4"}')
        self.assertEqual(rec.last_modified, '1618846125.126554')
        self.assertEqual(rec.vtag, b'5kzmcxRpTdtQFl0IIuAbkF')

    def test_empty(self):
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test2",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        self.assertTrue(rec.empty)

    def test_delete(self):
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test3",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        self.assertFalse(rec.empty)
        self.assertTrue(rec.is_delete)

if __name__ == '__main__':
    unittest.main()