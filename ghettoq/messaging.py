from Queue import Empty
from itertools import cycle


class Queue(object):

    def __init__(self, backend, name):
        self.name = name
        self.backend = backend

    def put(self, payload):
        self.backend.put(self.name, payload)

    def get(self):
        payload = self.backend.get(self.name)
        if payload is not None:
            return payload
        raise Empty



class QueueSet(object):

    def __init__(self, backend, queues):
        self.backend = backend
        self.queue_names = queues
        self.queues = map(self.backend.Queue, self.queue_names)
        self.cycle = cycle(self.queues)
        self.all = frozenset(self.queue_names)

    def get(self):
        tried = set()

        while True:
            queue = self.cycle.next()
            try:
                return queue.get()
            except QueueEmpty:
                tried.add(queue)
                if tried == self.all:
                    raise
