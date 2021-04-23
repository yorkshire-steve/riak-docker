from sink import ReplSink
import boto3
import os

class App:

    def __init__(self):
        self.host = os.getenv('RIAK_HOST', 'localhost')
        self.port = os.getenv('RIAK_PORT', '8098')

    def main(self):
        return

if __name__ == '__main__':
    App().main()