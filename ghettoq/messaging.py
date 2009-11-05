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
        self.queues = map(self.backend.Queue, queues)
        self.cycle = cycle(self.queues)

    def get(self):
        while True:
            try:
                return self.cycle.next().get()
            except QueueEmpty:
                pass
