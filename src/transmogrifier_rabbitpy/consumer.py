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
import time


def get_item(message):
    content_type = message.properties.get('content_type')
    content_encoding = message.properties.get('content_encoding')

    if content_type == 'application/json':
        return message.json()

    elif content_type == 'application/x-msgpack':
        return msgpack.unpackb(message.body)

    elif content_type == 'application/x-pickle':
        if content_encoding == '':
            return cPickle.loads(message.body)
        elif content_encoding == 'gzip':
            return cPickle.loads(decompress(message.body))

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

        options = dict([(k.replace('-', '_'), v)
                        for k, v in self.options.items()])

        amqp_uri = options.get(
            'amqp_uri',
            'amqp://guest:guest@localhost:5672/%2f'
        )
        queue = options.get('queue', '')
        exchange = options.get('exchange', 'amq.topic')
        routing_key = options.get('routing_key', '*')

        exchange_options = {}
        for k, v in options.items():
            v = to_boolean_when_looks_boolean(v)
            if k.startswith('exchange_'):
                exchange_options[k[len('exchange_'):]] = v

        queue_options = {
            'auto_declare': True,
            'auto_delete': True
        }

        # Should the message be acked; False is useful during development
        ack = to_boolean_when_looks_boolean(options.get('ack', 'true'))

        for k, v in options.items():
            v = to_boolean_when_looks_boolean(v)
            if k.startswith('queue_'):
                queue_options[k[len('queue_'):]] = v

        key = self.options.get('key')

        # Connect to RabbitMQ on localhost, port 5672 as guest/guest
        with rabbitpy.Connection(amqp_uri) as conn1, \
             rabbitpy.Connection(amqp_uri) as conn2:

            # Open the channel to communicate with RabbitMQ
            with conn1.channel() as channel1, \
                 conn2.channel() as channel2:

                if ack:
                    channel1.prefetch_count(1)

                exchange_declare = exchange_options.pop('auto_declare', False)
                if exchange_declare:
                    exchange = rabbitpy.Exchange(channel1, **exchange_options)
                    exchange.declare()

                queue_declare = queue_options.pop('auto_declare', True)
                queue1 = rabbitpy.Queue(channel1, queue, **queue_options)
                queue2 = rabbitpy.Queue(channel2, queue, **queue_options)
                if queue_declare:
                    queue1.declare()

                queue1.bind(exchange, routing_key)

                seen = {}

                # Exit on CTRL-C or queue1 empty for 30 sec
                counter = 0
                try:
                    # Consume the message
                    print('Waiting for a new message...')
                    for message in queue1:
                        counter = counter + 1
                        print(('Received a new message ({0:d}). '
                               'Processing...'.format(counter)))

                        if key:
                            yield {key: get_item(message)}
                        else:
                            yield get_item(message)
                        if ack:
                            message.ack()

                        if not(len(queue2)):
                            print('Waiting for a new message...')

                        # Break when no new messages in 30 seconds
                        grace = 30
                        while(grace > 0 and not len(queue2)):
                            grace = grace - 1
                            time.sleep(1)
                        if grace < 1:
                            break

                except KeyboardInterrupt:
                    print('Consumer stopped. Exiting...')
                except Exception:
                    raise Exception(traceback.format_exc())
