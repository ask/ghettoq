from ghettoq.messaging import Queue, Empty
from ghettoq.backends import Connection
from anyjson import serialize, deserialize
import unittest


class TestRedisBackend(unittest.TestCase):

    def test_basic(self):
        b = Connection("redis", database="ghettoq-test-queue")
        q = Queue("testing", backend=b)

        self.assertRaises(Empty, q.get)
        q.put(serialize({"name": "George Constanza"}))

        self.assertEquals(deserialize(q.get()),
                {"name": "George Constanza"})
