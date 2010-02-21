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
        return self.client.rpop(queue)

    def purge(self, queue):
        return self.client.delete(queue)
