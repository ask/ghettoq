from carrot.backends.base import BaseBackend, BaseMessage
from anyjson import serialize, deserialize
from ghettoq.backends import Connection
from itertools import count
from ghettoq.messaging import Empty as QueueEmpty
from django.utils.datastructures import SortedDict
from carrot.utils import gen_unique_id
import sys
import time
import atexit


class QualityOfService(object):

    def __init__(self, resource, prefetch_count=None, interval=None):
        self.resource = resource
        self.prefetch_count = prefetch_count
        self.interval = interval
        self._delivered = SortedDict()
        self._restored_once = False
        atexit.register(self.restore_unacked_once)

    def can_consume(self):
        return len(self._delivered) > self.prefetch_count

    def append(self, message, queue_name, delivery_tag):
        self._delivered[delivery_tag] = message, queue_name

    def ack(self, delivery_tag):
        self._delivered.pop(delivery_tag, None)

    def restore_unacked(self):
        for message, queue_name in self._delivered.items():
            self.resource.put(queue_name, message)
        self._delivered = SortedDict()

    def requeue(self, delivery_tag):
        try:
            message, queue_name = self._delivered.pop(delivery_tag)
        except KeyError:
            pass
        self.resource.put(queue_name, message)

    def restore_unacked_once(self):
        if not self._restored_once:
            if self._delivered:
                sys.stderr.write(
                    "Restoring unacknowledged messages: %s\n" % (
                        self._delivered))
            self.restore_unacked()
            if self._delivered:
                sys.stderr.write("UNRESTORED MESSAGES: %s\n" % (
                    self._delivered))


class Message(BaseMessage):

    def __init__(self, backend, payload, **kwargs):
        self.backend = backend

        payload = deserialize(payload)
        kwargs["body"] = payload.get("body").encode("utf-8")
        kwargs["delivery_tag"] = payload.get("delivery_tag")
        kwargs["content_type"] = payload.get("content-type")
        kwargs["content_encoding"] = payload.get("content-encoding")
        kwargs["priority"] = payload.get("priority")
        self.destination = payload.get("destination")

        super(Message, self).__init__(backend, **kwargs)

    def reject(self):
        raise NotImplementedError(
            "The GhettoQ backend does not implement basic.reject")


class MultiBackend(BaseBackend):
    Message = Message
    default_port = None
    type = None
    interval = 1
    _prefetch_count = None

    def __init__(self, connection, **kwargs):
        if not self.type:
            raise NotImplementedError(
                        "MultiBackends must have the type attribute")
        self.connection = connection
        self._consumers = {}
        self._callbacks = {}
        self._channel = None
        self._qos_manager = None

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
        self.channel.Queue(queue).purge()

    def _poll(self, resource):
        while True:
            if self.qos_manager.can_consume():
                try:
                    return resource.get()
                except QueueEmpty:
                    pass
            time.sleep(self.interval)

    def declare_consumer(self, queue, no_ack, callback, consumer_tag,
                         **kwargs):
        self._consumers[consumer_tag] = queue
        self._callbacks[queue] = callback

    def consume(self, limit=None):
        queueset = self.channel.QueueSet(self._consumers.values())

        for total_message_count in count():

            if limit and total_message_count >= limit:
                raise StopIteration

            payload, queue = self._poll(queueset)

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

    def ack(self, delivery_tag):
        self.qos_manager.ack(delivery_tag)

    def requeue(self, delivery_tag):
        self.qos_manager.requeue(delivery_tag)

    def message_to_python(self, raw_message):
        message = self.Message(backend=self, payload=raw_message)
        self.qos_manager.append(message, message.destination,
                                message.delivery_tag)
        return message

    def prepare_message(self, message_data, delivery_mode, priority=0,
            content_type=None, content_encoding=None):
        return {"body": message_data,
                "delivery_tag": gen_unique_id(),
                "priority": priority or 0,
                "content-encoding": content_encoding,
                "content-type": content_type}

    def publish(self, message, exchange, routing_key, **kwargs):
        message["destination"] = exchange
        self.channel.Queue(exchange).put(serialize(message))

    def cancel(self, consumer_tag):
        if not self._channel:
            return
        queue = self._consumers.pop(consumer_tag, None)
        self._callbacks.pop(queue, None)

    def close(self):
        for consumer_tag in self._consumers.keys():
            self.cancel(consumer_tag)
        if self._channel:
            self._channel.close()
        self._channel = None

    def basic_qos(self, prefetch_size, prefetch_count, apply_global=False):
        self._prefetch_count = prefetch_count

    @property
    def channel(self):
        if not self._channel:
            # Need one connection per channel.
            # AMQP has multiplexing, but Redis does not.
            self._channel = self.establish_connection()
        return self._channel

    @property
    def qos_manager(self):
        if self._qos_manager is None:
            self._qos_manager = QualityOfService(self.channel)

        # Update prefetch count / interval
        self._qos_manager.prefetch_count = self._prefetch_count
        self._qos_manager.interval = self.interval

        return self._qos_manager


class Redis(MultiBackend):
    type = "Redis"


class Database(MultiBackend):
    type = "database"
