from redis import Redis as Redis
from ghettoq.backends.base import BaseBackend

DEFAULT_PORT = 6379


class RedisBackend(BaseBackend):

    def establish_connection(self):
        self.port = self.port or DEFAULT_PORT
        return Redis(host=self.host, port=self.port, db=self.database,
                     password=self.password)

    def put(self, queue, message):
        self.client.lpush(queue, message)

    def get(self, queue):
        dest, item = self.client.brpop([queue], timeout=1)
        return item

    def get_many(self, queues, timeout=None):
        dest, item = self.client.brpop(queues, timeout)
        return item, dest

    def purge(self, queue):
        return self.client.delete(queue)
