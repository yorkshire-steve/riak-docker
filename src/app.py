from sink import ReplSink
from record import ReplRecord
from boto3 import resource
from boto3.dynamodb.conditions import Attr
from botocore.config import Config
import os
import time
import signal
import logging
import json
from urllib3.exceptions import HTTPError
from decimal import Decimal

class App:
    def __init__(self):
        self.shutdown = False
        self.logger = self.get_logger()
        self.sink = self.setup_riak_sink()
        self.table = self.setup_dynamodb_table()

    def get_logger(self):
        logger = logging.getLogger()
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)
        return logger

    def setup_riak_sink(self):
        host = os.getenv('RIAK_HOST', 'localhost')
        port = int(os.getenv('RIAK_PORT', '8098'))
        queue_name = os.getenv('RIAK_QUEUE', 'q1_ttaaefs')
        self.bucket_filter = os.getenv('RIAK_BUCKET', 'test')
        self.logger.info(f"Setting up replication sink from host={host} port={port} queue_name={queue_name}")
        return ReplSink(host=host, port=port, queue=queue_name)

    def setup_dynamodb_table(self):
        connect_timeout = int(os.getenv('DYNAMODB_CONNECT_TIMEOUT', '1'))
        read_timeout = int(os.getenv('DYNAMODB_READ_TIMEOUT', '1'))
        retries = int(os.getenv('DYNAMODB_RETRIES', '1'))
        config = Config(connect_timeout=connect_timeout, read_timeout=read_timeout, retries={'max_attempts': retries})
        endpoint_url = os.getenv('DYNAMODB_ENDPOINT_URL')
        table_name = os.getenv('DYNAMODB_TABLE', 'test')
        self.logger.info(f"Setting up dynamodb url={endpoint_url} connect_timeout={connect_timeout} read_timeout={read_timeout} retries={retries} table={table_name}")
        if endpoint_url:
            dynamodb = resource('dynamodb', endpoint_url=endpoint_url, config=config)
        else:
            dynamodb = resource('dynamodb', config=config)

        table = dynamodb.Table(table_name)
        try:
            table.load()
        except table.meta.client.exceptions.ResourceNotFoundException:
            self.logger.info(f"Table {table_name} does not exist, creating")
            dynamodb.create_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {'AttributeName': 'pkey','AttributeType': 'S'}
                ],
                KeySchema=[
                    {'AttributeName': 'pkey', 'KeyType': 'HASH'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5,'WriteCapacityUnits': 5}
            )
            table.wait_until_exists()
        return table

    def update_item(self, key: str, rec: ReplRecord):
        try:
            data = json.loads(rec.value)
            data['pkey'] = key
            data['_riak_lm'] = Decimal(rec.last_modified)
            data['_riak_vclocks'] = rec.vector_clocks.decode('utf-8')
            self.logger.info(f"Putting item {key}")
            condition = Attr('_riak_lm').lt(data['_riak_lm']) | Attr('_riak_lm').not_exists()
            self.table.put_item(Item=data, ConditionExpression=condition)
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            self.logger.warning(f"Put for key={key} failed due to existing last modified > {data['_riak_lm']}")
        except Exception as e:
            self.logger.error(e)

    def process_record(self, rec: ReplRecord):
        bucket = rec.bucket.decode('utf-8')
        key = rec.key.decode('utf-8')
        if {b'content-type': b'application/json'} in rec.metadata and bucket == self.bucket_filter:
            self.update_item(key, rec)
        else:
            self.logger.warning(f"Key not JSON or wrong bucket {bucket} {key}")

    def signal_handler(self, sign_num, frame):
        self.shutdown = True

    def main(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info("Starting consume from queue")

        riak_failure = False
        while not self.shutdown:
            try:
                rec = self.sink.fetch()
            except HTTPError as e:
                self.logger.error(e)
                self.logger.warning("Riak failure, backing off for 5 seconds")
                riak_failure = True
                time.sleep(5)
            except Exception as e:
                self.logger.warning(e)
            else:
                if riak_failure:
                    self.logger.info("Recovered from Riak failure")
                    riak_failure = False
                if not rec.empty:
                    self.process_record(rec)
                if rec.empty:
                    time.sleep(0.1)
        
        self.logger.info("Safe shutdown, goodbye.")

if __name__ == '__main__':
    App().main()