from ghettoq.simple import Connection, Empty
from anyjson import serialize, deserialize
import unittest


class TestDatabaseBackend(unittest.TestCase):

    def test_basic(self):
        conn = Connection("database")
        q = conn.Queue("testing")

        self.assertRaises(Empty, q.get)
        q.put(serialize({"name": "George Constanza"}))

        self.assertEquals(deserialize(q.get()),
                {"name": "George Constanza"})
