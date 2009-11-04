from Queue import Empty


class Queue(object):

    def __init__(self, name, backend):
        self.name = name
        self.backend = backend

    def put(self, payload):
        self.backend.put(self.name, payload)

    def get(self):
        payload = self.backend.get(self.name)
        if payload is not None:
            return payload
        raise Empty
