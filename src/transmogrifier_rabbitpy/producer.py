# -*- coding: utf-8 -*-
from venusianconfiguration import configure

from transmogrifier.blueprints import Blueprint
import rabbitpy


@configure.transmogrifier.blueprint.component(name='rabbitpy.producer')
class Producer(Blueprint):
    def __iter__(self):
        amqp_uri = self.options.get(
            'amqp_uri',
            'amqp://guest:guest@localhost:5672/%2f'
        )
        exchange = self.options.get('exchange', 'amq.topic')
        routing_key = self.options.get('routing_key', '*')

        # Connect to RabbitMQ on localhost, port 5672 as guest/guest
        with rabbitpy.Connection(amqp_uri) as conn:

            # Open the channel to communicate with RabbitMQ
            with conn.channel() as channel:

                for item in self.previous:
                    message = rabbitpy.Message(channel, item)
                    message.publish(exchange, routing_key)
                    yield item
