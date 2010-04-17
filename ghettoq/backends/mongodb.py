from pymongo.connection import Connection
from ghettoq.backends.base import BaseBackend
from ghettoq.messaging import Empty

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 27017

class MongodbBackend(BaseBackend):

    def __init__(self, *args, **kwargs):
        super(MongodbBackend, self).__init__(*args, **kwargs)

        self.port = self.port or DEFAULT_PORT
        self.host = self.host or DEFAULT_HOST
        self.connection = Connection(host=self.host, port=self.port)
        self.database = getattr(self.connection, (self.database == "/" and
"ghettoq") or self.database)

    def put(self, queue, message):
        getattr(self.database, queue).insert({"payload" : message})

    def get(self, queue):
        msg =  getattr(self.database, queue).find_one()
        getattr(self.database, queue).remove(msg)
        if not msg:
            raise Empty, "Empty queue"
        return msg["payload"]

    def purge(self, queue):
        return getattr(self.database, queue).remove()
