# -*- coding: utf-8 -*-
from email.generator import Generator
from email.message import Message
from tarfile import TarFile
from zlib import compress
import traceback

from venusianconfiguration import configure
import rabbitpy
from transmogrifier.blueprints import ConditionalBlueprint
from transmogrifier_rabbitpy.utils import to_boolean_when_looks_boolean

import msgpack
import cPickle


try:
    from StringIO import StringIO as BytesIO
    # BytesIO is not safe with Python < 3, because Message may contain unicode
except ImportError:
    from io import BytesIO


def to_string(message):
    out = BytesIO()
    generator = Generator(out, mangle_from_=False, maxheaderlen=0)
    generator.flatten(message)
    return out.getvalue()


def create_message(channel, item, default_serializer='msgpack'):
    if isinstance(item, Message):
        return rabbitpy.Message(
            channel,
            compress(to_string(item)),
            properties=dict(
                content_type='message/rfc822',
                content_encoding='gzip'
            )
        )
    elif default_serializer == 'msgpack':
        return rabbitpy.Message(
            channel,
            msgpack.packb(item),
            properties=dict(
                content_type='application/x-msgpack',
                )
        )
    elif default_serializer == 'tarball':
        assert isinstance(item, TarFile), 'Item was not a TarFile'
        return rabbitpy.Message(
            channel,
            item,
            properties=dict(
                content_type='application/x-tar',
                )
        )
    elif default_serializer == 'pickle':
        return rabbitpy.Message(
            channel,
            compress(cPickle.dumps(item)),
            properties=dict(
                content_type='application/x-pickle',
                content_encoding='gzip'
            )
        )
    else:
        return rabbitpy.Message(channel, item)


@configure.transmogrifier.blueprint.component(name='rabbitpy.producer')
class Producer(ConditionalBlueprint):
    def __iter__(self):
        options = dict([(key.replace('-', '_'), value)
                        for key, value in self.options.items()])
        # Serializer
        default_serializer = options.get('serializer', 'json')

        # URI
        amqp_uri = options.get(
            'amqp_uri',
            'amqp://guest:guest@localhost:5672/%2f'
        )

        # Exchange
        exchange = options.get('exchange', 'amq.topic')
        exchange_options = {}
        for k, v in options.items():
            v = to_boolean_when_looks_boolean(v)
            if k.startswith('exchange_'):
                exchange_options[k[len('exchange_'):]] = v

        # Queue
        queue = options.get('queue', '')
        queue_options = {
            'auto_declare': True,
            'auto_delete': True
        }
        for k, v in options.items():
            v = to_boolean_when_looks_boolean(v)
            if k.startswith('queue_'):
                queue_options[k[len('queue_'):]] = v

        # Publisher confirms
        publisher_confirms = to_boolean_when_looks_boolean(
            options.get('publisher_confirms'))

        # Routing key
        routing_key = options.get('routing_key', '*')

        # Item key
        key = options.get('key')

        # Or multiple keys
        keys = options.get('keys')

        # Connect to RabbitMQ on localhost, port 5672 as guest/guest
        with rabbitpy.Connection(amqp_uri) as conn:

            # Open the channel to communicate with RabbitMQ
            with conn.channel() as channel:

                # Turn on publisher confirmations
                if publisher_confirms:
                    channel.enable_publisher_confirms()

                # Declare exchange
                exchange_declare = exchange_options.pop('auto_declare', False)
                if exchange_declare:
                    exchange = rabbitpy.Exchange(channel, **exchange_options)
                    exchange.declare()

                # Declare queue
                if queue:
                    queue_declare = queue_options.pop('auto_declare', True)
                    queue = rabbitpy.Queue(channel, queue, **queue_options)
                    if queue_declare:
                        queue.declare()
                    queue.bind(exchange, routing_key)

                # Publish
                try:
                    for item in self.previous:
                        if self.condition(item):
                            if key is not None:
                                message = create_message(channel, item[key],
                                                         default_serializer)
                            elif keys is not None:
                                message = create_message(channel, dict(
                                    [(k, v) for k, v in item.items()
                                     if k in keys]), default_serializer)
                            else:
                                message = create_message(channel, item,
                                                         default_serializer)
                            if publisher_confirms:
                                if not message.publish(exchange, routing_key):
                                    raise Exception('NO ROUTE')
                            else:
                                message.publish(exchange, routing_key)
                        yield item
                except Exception:
                    raise Exception(traceback.format_exc())
