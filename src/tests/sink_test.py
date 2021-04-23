import unittest
from sink import ReplSink
import urllib3
import time
import os

class TestReplSink(unittest.TestCase):

    def setUp(self):
        """
        Setup sink and confirm replication queue is empty
        """
        self.host = os.environ['RIAK_HOST']
        self.sink = ReplSink(host=self.host, port=8098, queue='q1_ttaaefs')
        self.test_data = b'{"test":"data"}'
        self.http = urllib3.PoolManager()

        empty = False
        while not empty:
            rec = self.sink.fetch()
            empty = rec.empty

    def tearDown(self):
        """
        Assert replication queue is empty after every test
        """
        rec = self.sink.fetch()
        assert rec.empty

    def put_test_object(self, bucket: str, key: str):
        url = f"http://{self.host}:8098/buckets/{bucket}/keys/{key}"
        headers = {'Content-type':'application/json'}
        self.http.request("PUT", url, headers=headers, body=self.test_data, retries=False)

    def delete_test_object(self, bucket: str, key: str):
        url = f"http://{self.host}:8098/buckets/{bucket}/keys/{key}"
        self.http.request("DELETE", url, retries=False)

    def assert_test_record(self, rec, bucket, key):
        self.assertFalse(rec.empty)
        self.assertFalse(rec.is_delete)
        self.assertIsNone(rec.bucket_type)
        self.assertEqual(rec.bucket, bucket)
        self.assertEqual(rec.key, key)
        self.assertEqual(rec.value, self.test_data)
        self.assertIn({b'content-type': b'application/json'}, rec.metadata)

    def assert_empty_record(self, rec):
        self.assertTrue(rec.empty)
        self.assertIsNone(rec.bucket)
        self.assertIsNone(rec.key)
        self.assertIsNone(rec.value)

    def assert_delete_record(self, rec, bucket, key):
        self.assertFalse(rec.empty)
        self.assertTrue(rec.is_delete)
        self.assertEqual(rec.bucket, bucket)
        self.assertEqual(rec.key, key)
        self.assertEqual(rec.value, b'')

    def test_empty(self):
        """
        Test that the replication queue is empty
        """
        rec = self.sink.fetch()
        self.assert_empty_record(rec)

    def test_single_put(self):
        """
        Test a single PUT is fetchd from replication queue
        """
        bucket = b'testBucket'
        key = b'testKey'
        self.put_test_object(bucket.decode('utf-8'), key.decode('utf-8'))
        time.sleep(0.05)
        rec = self.sink.fetch()
        self.assert_test_record(rec, bucket, key)

    def test_multiple_put(self):
        """
        Test multiple PUTs are fetchd from replication queue
        """
        for i in range(10):
            bucket = 'testBucket'
            key = f'testKey{i}'
            self.put_test_object(bucket, key)

        time.sleep(0.05)

        for i in range(10):
            bucket = b'testBucket'
            key = f'testKey{i}'.encode('utf-8')
            rec = self.sink.fetch()
            self.assert_test_record(rec, bucket, key)

    def test_single_delete(self):
        """
        Test a single DELETE is fetchd from replication queue
        """
        bucket = b'testBucket'
        key = b'testKey'
        self.put_test_object(bucket.decode('utf-8'), key.decode('utf-8'))
        time.sleep(0.05)
        rec = self.sink.fetch()

        self.delete_test_object(bucket.decode('utf-8'), key.decode('utf-8'))
        time.sleep(0.05)
        rec = self.sink.fetch()
        self.assert_delete_record(rec, bucket, key)

    def test_last_modified(self):
        """
        Test correct last modified timestamp on a single PUT
        """
        bucket = b'testBucket'
        key = b'testKey'
        before_time = time.time()
        self.put_test_object(bucket.decode('utf-8'), key.decode('utf-8'))
        after_time = time.time()

        time.sleep(0.05)
        rec = self.sink.fetch()
        self.assertGreaterEqual(float(rec.last_modified), before_time)
        self.assertLessEqual(float(rec.last_modified), after_time)

if __name__ == '__main__':
    unittest.main()