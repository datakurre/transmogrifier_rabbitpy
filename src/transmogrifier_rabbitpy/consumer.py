# -*- coding: utf-8 -*-
from __future__ import print_function
from email import message_from_string
import traceback
from zlib import decompress
import cPickle

from venusianconfiguration import configure
from transmogrifier.blueprints import Blueprint
from transmogrifier_rabbitpy.utils import to_boolean_when_looks_boolean
import rabbitpy

import msgpack


def get_message_body(message):
    content_type = message.properties.get('content_type')
    content_encoding = message.properties.get('content_encoding')

    if content_type == 'application/x-pickle':
        return cPickle.loads(message.body)

    elif content_type == 'application/x-msgpack':
        return msgpack.unpackb(message.body)

    elif content_type == 'application/json':
        return message.json()

    elif content_type == 'message/rfc822':
        if content_encoding == '':
            return message_from_string(message.body)
        elif content_encoding == 'gzip':
            return message_from_string(decompress(message.body))

    raise Exception(('Unknown content-type \'{0:s}\' '
                     'with encoding \'{1:s}\'').format(content_type,
                                                       content_encoding))


@configure.transmogrifier.blueprint.component(name='rabbitpy.consumer')
class Consumer(Blueprint):
    def __iter__(self):
        for item in self.previous:
            yield item

        options = dict([(key.replace('-', '_'), value)
                        for key, value in self.options.items()])

        amqp_uri = options.get(
            'amqp_uri',
            'amqp://guest:guest@localhost:5672/%2f'
        )
        queue = options.get('queue', '')
        exchange = options.get('exchange', 'amq.topic')
        routing_key = options.get('routing_key', '*')

        exchange_options = {}
        for key, value in options.items():
            value = to_boolean_when_looks_boolean(value)
            if key.startswith('exchange_'):
                exchange_options[key[len('exchange_'):]] = value

        queue_options = {
            'auto_declare': True,
            'auto_delete': True
        }

        # Should the message be acked; False is useful during development
        ack = to_boolean_when_looks_boolean(options.get('ack', 'true'))

        for key, value in options.items():
            value = to_boolean_when_looks_boolean(value)
            if key.startswith('queue_'):
                queue_options[key[len('queue_'):]] = value

        # Connect to RabbitMQ on localhost, port 5672 as guest/guest
        with rabbitpy.Connection(amqp_uri) as conn:

            # Open the channel to communicate with RabbitMQ
            with conn.channel() as channel:

                exchange_declare = exchange_options.pop('auto_declare', False)
                if exchange_declare:
                    exchange = rabbitpy.Exchange(channel, **exchange_options)
                    exchange.declare()

                queue_declare = queue_options.pop('auto_declare', True)
                queue = rabbitpy.Queue(channel, queue, **queue_options)
                if queue_declare:
                    queue.declare()

                queue.bind(exchange, routing_key)

                # Exit on CTRL-C
                counter = 0
                try:
                    # Consume the message
                    print('Waiting for a new message...')
                    for message in queue:
                        counter += 1
                        print(('Received a new message ({0:d}). '
                               'Processing...'.format(counter)))
                        yield(get_message_body(message))
                        if ack:
                            message.ack()
                        print('Waiting for a new message...')
                except KeyboardInterrupt:
                    print('Consumer stopped. Exiting...')
                except Exception:
                    raise Exception(traceback.format_exc())
