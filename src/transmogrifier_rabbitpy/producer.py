# -*- coding: utf-8 -*-
from email.generator import Generator
from email.message import Message
from io import BytesIO
from zlib import compress
from venusianconfiguration import configure

from transmogrifier.blueprints import ConditionalBlueprint
from transmogrifier_rabbitpy.utils import to_boolean_when_looks_boolean

import rabbitpy
import traceback 


def to_string(message):
    out = BytesIO()
    generator = Generator(out, mangle_from_=False, maxheaderlen=0)
    generator.flatten(message)
    return out.getvalue()


def create_message(channel, item):
    if isinstance(item, dict):
        return rabbitpy.Message(channel, item)
    elif isinstance(item, Message):
        return rabbitpy.Message(
            channel,
            compress(to_string(item)),
            properties=dict(
                content_type='message/rfc822',
                content_encoding='gzip'
            )
        )


@configure.transmogrifier.blueprint.component(name='rabbitpy.producer')
class Producer(ConditionalBlueprint):
    def __iter__(self):
        # URI
        amqp_uri = self.options.get(
            'amqp_uri',
            'amqp://guest:guest@localhost:5672/%2f'
        )

        # Exchange
        exchange = self.options.get('exchange', 'amq.topic')
        exchange_options = {}
        for key, value in self.options.items():
            value = to_boolean_when_looks_boolean(value)
            if key.startswith('exchange_'):
                exchange_options[key[len('exchange_'):]] = value

        # Publisher confirms
        publisher_confirms = to_boolean_when_looks_boolean(
            self.options.get('publisher_confirms'))

        # Routing key
        routing_key = self.options.get('routing_key', '*')

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

                # Publish
                try:
                    for item in self.previous:
                        if self.condition(item):
                            message = create_message(channel, item)
                            if publisher_confirms:
                                if not message.publish(exchange, routing_key):
                                    raise Exception('NO ROUTE')
                            else:
                                message.publish(exchange, routing_key)
                        yield item
                except Exception as e:
                    raise Exception(traceback.format_exc())
