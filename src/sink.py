import urllib3
from record import ReplRecord

class ReplSink:

    def __init__(self, host: str, port: int, queue: str):
        self._host = host
        self._port = port
        self._queue_name = queue
        self._url = f"http://{self._host}:{self._port}/queuename/{self._queue_name}?object_format=internal"
        self._http = urllib3.HTTPConnectionPool(host=self._host, port=self._port, retries=False)
    
    def fetch(self):
        r = self._http.request("GET", self._url)
        if r.status != 200:
            raise urllib3.exceptions.HTTPError(f"invalid http response code {r.status}")

        return ReplRecord(r.data)
