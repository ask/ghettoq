from ghettoq.simple import Connection, Empty
from anyjson import serialize, deserialize
import unittest


def create_connection(database):
    return Connection("redis", database=database)


class TestRedisBackend(unittest.TestCase):

    def test_empty_raises_Empty(self):
        conn = create_connection("test_empty_raises_Empty")
        q = conn.Queue("testing")

        self.assertRaises(Empty, q.get)

    def test_put__get(self):
        conn = create_connection("test_put__get")
        q = conn.Queue("testing")
        q.put(serialize({"name": "George Constanza"}))

        self.assertEquals(deserialize(q.get()),
                {"name": "George Constanza"})

    def test_empty_queueset_raises_Empty(self):
        conn = create_connection("test_empty_queueset_raises_Empty")
        a, b, c, = conn.Queue("a"), conn.Queue("b"), conn.Queue("c")
        queueset = conn.QueueSet(queue.name for queue in (a, b, c))
        for queue in a, b, c:
            self.assertRaises(Empty, queue.get)
        self.assertRaises(Empty, queueset.get)
