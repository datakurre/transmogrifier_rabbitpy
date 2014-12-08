# -*- coding: utf-8 -*-
from venusianconfiguration import configure

from transmogrifier.blueprints import Blueprint
import rabbitpy


@configure.transmogrifier.blueprint.component(name='rabbitpy.consumer')
class Consumer(Blueprint):
    def __iter__(self):
        for item in self.previous:
            yield item

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

                queue = rabbitpy.Queue(channel, auto_delete=True)
                queue.declare()

                queue.bind(exchange, routing_key)

                # Exit on CTRL-C
                try:
                    # Consume the message
                    for message in queue:
                        message.ack()
                        yield(message.json())
                except KeyboardInterrupt:
                    print 'Exited consumer'
