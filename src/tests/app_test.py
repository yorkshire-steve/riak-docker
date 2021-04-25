import unittest
from app import App
from sink import ReplSink
from record import ReplRecord
import time
import os
import boto3
from decimal import Decimal
from unittest.mock import Mock

class TestApp(unittest.TestCase):

    def setUp(self):
        """
        Setup app
        """
        self.host = os.getenv('RIAK_HOST', 'localhost')
        self.port = int(os.getenv('RIAK_PORT', '8098'))
        self.queue_name = os.getenv('RIAK_QUEUE', 'q1_ttaaefs')
        self.table_name = os.getenv('DYNAMODB_TABLE', 'test')
        self.endpoint_url = os.getenv('DYNAMODB_ENDPOINT_URL')
        self.test_data = b'{"test":"data"}'

    def tearDown(self):
        """
        Tear down
        """
        pass

    def test_setup_riak_sink(self):
        app = App()
        app.logger = Mock()
        app.logger.info = Mock()
        sink = app.setup_riak_sink()
    
        host = os.getenv('RIAK_HOST', 'localhost')
        port = int(os.getenv('RIAK_PORT', '8098'))
        queue_name = os.getenv('RIAK_QUEUE', 'q1_ttaaefs')

        self.assertIsInstance(sink, ReplSink)
        self.assertEqual(sink._host, host)
        self.assertEqual(sink._port, port)
        self.assertEqual(sink._queue_name, queue_name)
        app.logger.info.assert_any_call(f"Setting up replication sink from host={self.host} port={self.port} queue_name={self.queue_name}")

    def test_setup_dynamodb_table(self):
        dynamodb = boto3.resource('dynamodb', endpoint_url=self.endpoint_url)
        try:
            table = dynamodb.Table(self.table_name)
            table.load()
            table.delete()
            table.wait_until_not_exists()
        except:
            pass

        app = App()
        app.logger = Mock()
        app.logger.info = Mock()
        app.setup_dynamodb_table()

        response = dynamodb.meta.client.describe_table(TableName=self.table_name)
        self.assertEqual(response['Table']['TableName'], self.table_name)
        self.assertEqual(response['Table']['KeySchema'], [{'AttributeName': 'pkey', 'KeyType': 'HASH'}])
        app.logger.info.assert_any_call(f"Table {self.table_name} does not exist, creating")

    def test_update_item(self):
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        app = App()
        app.logger = Mock()
        app.table = app.setup_dynamodb_table()
        app.table.delete_item(Key={'pkey':'test'})
        time.sleep(0.05)

        app.update_item('test', rec)

        item = app.table.get_item(Key={'pkey':'test'})

        self.assertEqual(item['Item']['pkey'], 'test')
        self.assertEqual(item['Item']['_riak_lm'], Decimal('1618846125.126554'))
        self.assertEqual(item['Item']['_riak_vclocks'], 'g2wAAAACaAJtAAAACL8Aoe8A+zsmaAJhAm4FAHcc8tkOaAJtAAAADL8Aoe8A+0zuAAAAAWgCYQJuBQCtHfLZDmo=')
        self.assertEqual(item['Item']['test'], 'data4')

    def test_update_item_with_older(self):
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        app = App()
        app.logger = Mock()
        app.logger.warning = Mock()
        app.table = app.setup_dynamodb_table()
        app.table.put_item(Item={'pkey':'test', '_riak_lm': Decimal('1618954321.126554')})
        time.sleep(0.05)

        app.update_item('test', rec)

        item = app.table.get_item(Key={'pkey':'test'})

        self.assertEqual(item['Item']['pkey'], 'test')
        self.assertEqual(item['Item']['_riak_lm'], Decimal('1618954321.126554'))
        app.logger.warning.assert_called_with("Put for key=test failed due to existing last modified > 1618846125.126554")

    def test_delete_item(self):
        with open(os.path.dirname(os.path.abspath(__file__)) + "/data/test",'rb') as f:
            data = f.read()

        rec = ReplRecord(data)

        app = App()
        app.logger = Mock()
        app.table = app.setup_dynamodb_table()
        app.table.put_item(Item={'pkey':'test', '_riak_lm': Decimal('1618846000.126554')})
        time.sleep(0.05)

        app.delete_item('test', rec)

        item = app.table.get_item(Key={'pkey':'test'})
        
        self.assertNotIn('Item', item)

if __name__ == '__main__':
    unittest.main()