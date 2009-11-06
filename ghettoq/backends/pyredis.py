from redis import Redis as Redis
from ghettoq.backends.base import BaseBackend


class RedisBackend(BaseBackend):

    def establish_connection(self):
        return Redis(host=self.host, port=self.port, db=self.database,
                     timeout=self.timeout)

    def put(self, queue, message):
        self.client.push(queue, message, tail=True)

    def get(self, queue):
        return self.client.pop(queue)

    def purge(self, queue):
        return self.client.delete(queue)
