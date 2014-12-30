# -*- coding: utf-8 -*-
from email.generator import Generator
from email.message import Message
from zlib import compress
import traceback

from venusianconfiguration import configure
import rabbitpy
from transmogrifier.blueprints import ConditionalBlueprint
from transmogrifier_rabbitpy.utils import to_boolean_when_looks_boolean

import msgpack


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
        return rabbitpy.Message(channel, msgpack.packb(item),
                                properties={'content_type':
                                            'application/x-msgpack'})
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
        for key, value in options.items():
            value = to_boolean_when_looks_boolean(value)
            if key.startswith('exchange_'):
                exchange_options[key[len('exchange_'):]] = value

        # Queue
        queue = options.get('queue', '')
        queue_options = {
            'auto_declare': True,
            'auto_delete': True
        }
        for key, value in options.items():
            value = to_boolean_when_looks_boolean(value)
            if key.startswith('queue_'):
                queue_options[key[len('queue_'):]] = value

        # Publisher confirms
        publisher_confirms = to_boolean_when_looks_boolean(
            options.get('publisher_confirms'))

        # Routing key
        routing_key = options.get('routing_key', '*')

        # Item key
        key = options.get('key')

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
                            if key is None:
                                message = create_message(channel, item,
                                                         default_serializer)
                            else:
                                message = create_message(channel, item[key],
                                                         default_serializer)
                            if publisher_confirms:
                                if not message.publish(exchange, routing_key):
                                    raise Exception('NO ROUTE')
                            else:
                                message.publish(exchange, routing_key)
                        yield item
                except Exception:
                    raise Exception(traceback.format_exc())
