# -*- coding: utf-8 -*-
from venusianconfiguration import configure

from transmogrifier.blueprints import ConditionalBlueprint
from transmogrifier_rabbitpy.utils import to_boolean_when_looks_boolean

import rabbitpy


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
        confirms = to_boolean_when_looks_boolean(
            self.options.get('publisher_confirms'))

        # Routing key
        routing_key = self.options.get('routing_key', '*')

        # Fieldnames
        fieldnames = self.options.get('fieldnames')

        # Connect to RabbitMQ on localhost, port 5672 as guest/guest
        with rabbitpy.Connection(amqp_uri) as conn:

            # Open the channel to communicate with RabbitMQ
            with conn.channel() as channel:

                # Turn on publisher confirmations
                if confirms:
                    channel.enable_publisher_confirms()

                # Declare exchange
                exchange_declare = exchange_options.pop('auto_declare', False)
                if exchange_declare:
                    exchange = rabbitpy.Exchange(channel, **exchange_options)
                    exchange.declare()

                # Publish
                for item in self.previous:
                    if self.condition(item):
                        try:
                            if fieldnames:
                                message = rabbitpy.Message(channel, dict([
                                    (key, value) for key, value in item.items()
                                    if key in fieldnames
                                ]))
                            else:
                                message = rabbitpy.Message(channel, dict([
                                    (key, value) for key, value in item.items()
                                    if not key.startswith('_')
                                ]))
                        except Exception as e:
                            raise Exception(e)
                        if confirms:
                            if not message.publish(exchange, routing_key):
                                raise Exception('NO ROUTE')
                        else:
                            message.publish(exchange, routing_key)
                    yield item
