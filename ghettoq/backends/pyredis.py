from redis import Redis as Redis
from ghettoq.backends.base import BaseBackend


class RedisBackend(BaseBackend):

    def establish_connection(self):
        return Redis(host=self.host, port=self.port, db=self.database,
                     password=self.password)

    def put(self, queue, message):
        self.client.push(queue, message, head=False)

    def get(self, queue):
        return self.client.pop(queue)

    def purge(self, queue):
        return self.client.delete(queue)
