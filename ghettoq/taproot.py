from carrot.backends.base import BaseBackend, BaseMessage
from anyjson import serialize, deserialize
from ghettoq.backends import Connection
from itertools import count
from ghettoq.messaging import Empty as QueueEmpty
from uuid import uuid4


class Message(BaseMessage):

    def __init__(self, backend, payload, **kwargs):
        self.backend = backend

        payload = deserialize(payload)
        kwargs["body"] = payload.get("body")
        kwargs["delivery_tag"] = payload.get("delivery_tag")
        kwargs["content_type"] = payload.get("content-type")
        kwargs["content_encoding"] = payload.get("content-encoding")
        kwargs["priority"] = payload.get("priority")

        super(Message, self).__init__(backend, **kwargs)

    def ack(self):
        pass

    def reject(self):
        raise NotImplementedError(
            "The GhettoQ backend does not implement basic.reject")

    def requeue(self):
        raise NotImplementedError(
            "The GhettoQ backend does not implement requeue")


class Backend(BaseBackend):
    Message = Message
    default_port = None
    type = "Redis"

    def __init__(self, connection, **kwargs):
        self.type = kwargs.get("type", self.type)
        self.connection = connection
        self._consumers = {}
        self._callbacks = {}
        self._channel = None

    def establish_connection(self):
        conninfo = self.connection
        return Connection(self.type, host=conninfo.hostname,
                                     user=conninfo.userid,
                                     password=conninfo.password,
                                     database=conninfo.virtual_host,
                                     port=conninfo.port)

    def close_connection(self, connection):
        connection.close()

    def queue_exists(self, queue):
        return True

    def queue_purge(self, queue, **kwargs):
        # TODO
        pass

    def declare_consumer(self, queue, no_ack, callback, consumer_tag,
                         **kwargs):
        # FIXME
        self._consumers[consumer_tag] = queue
        self._callbacks[queue] = callback

    def consume(self, limit=None):
        queueset = self.channel.QueueSet(self._consumers.values())
        for total_message_count in count():
            if limit and total_message_count >= limit:
                raise StopIteration
            while True:
                try:
                    payload, queue = queueset.get()
                except QueueEmpty:
                    pass
                else:
                    break

            if not queue or queue not in self._callbacks:
                continue

            self._callbacks[queue](payload)

            yield True

    def queue_declare(self, queue, *args, **kwargs):
        pass

    def get(self, queue, **kwargs):
        try:
            payload = self.channel.Queue(queue).get()
        except QueueEmpty:
            return None
        else:
            return self.message_to_python(payload)

    def ack(self, frame):
        pass

    def message_to_python(self, raw_message):
        return self.Message(backend=self, payload=raw_message)

    def prepare_message(self, message_data, delivery_mode, priority=0,
            content_type=None, content_encoding=None):
        return {"body": message_data,
                "delivery_tag": str(uuid4()),
                "priority": priority or 0,
                "content-encoding": content_encoding,
                "content-type": content_type}

    def publish(self, message, exchange, routing_key, **kwargs):
        self.channel.Queue(exchange).put(serialize(message))

    def cancel(self, consumer_tag):
        if not self._channel:
            return
        self._consumers.pop(consumer_tag, None)

    def close(self):
        for consumer_tag in self._consumers.keys():
            self.cancel(consumer_tag)
        if self._channel:
            self._channel.close()
        self._channel = None

    @property
    def channel(self):
        if not self._channel:
            # Need one connection per channel (use AMQP if that is a problem)
            self._channel = self.establish_connection()
        return self._channel



