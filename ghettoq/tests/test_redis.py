from ghettoq.simple import Connection, Empty
from anyjson import serialize, deserialize
import unittest


class TestRedisBackend(unittest.TestCase):

    def test_basic(self):
        conn = Connection("redis", database="ghettoq-test-queue")
        q = conn.Queue("testing")

        self.assertRaises(Empty, q.get)
        q.put(serialize({"name": "George Constanza"}))

        self.assertEquals(deserialize(q.get()),
                {"name": "George Constanza"})
