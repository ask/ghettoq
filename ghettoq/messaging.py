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

    def purge(self):
        self.backend.purge(self.name)

    def __repr__(self):
        return "<Queue: %s>" % repr(self.name)


class QueueSet(object):
    """A set of queues that operates as one."""

    def __init__(self, backend, queues):
        self.backend = backend
        self.queue_names = list(queues)

        # queues could be a PriorityQueue as well to support
        # priorities.
        self.queues = map(self.backend.Queue, self.queue_names)

        # an infinite cycle through all the queues.
        self.cycle = cycle(self.queues)

        # A set of all the queue names, so we can match when we've
        # tried all of them.
        self.all = frozenset(self.queue_names)

    def get(self):
        """Get the next message avaiable in the queue.

        :returns: The message and the name of the queue it came from as
            a tuple.
        :raises Empty: If there are no more items in any of the queues.

        """

        # A set of queues we've already tried.
        tried = set()

        while True:
            # Get the next queue in the cycle, and try to get an item off it.
            queue = self.cycle.next()
            try:
                item = queue.get()
            except Empty:
                # raises Empty when we've tried all of them.
                tried.add(queue.name)
                if tried == self.all:
                    raise
            else:
                return item, queue.name

    def __repr__(self):
        return "<QueueSet: %s>" % repr(self.queue_names)
