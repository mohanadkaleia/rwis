import random
import pika
import sys
import config
import json
import time
import asyncio

from concurrent.futures import ProcessPoolExecutor


LOCAL_QUEUE = 'task_queue'
REMOTE_QUEUE = 'message_queue'


class Station:
    def __init__(self, name='st0'):
        self.name = name

    def read(self):
        return random.choice(range(60, 100))


def produce():
    try:
        station_name = sys.argv[1]
        freq = int(sys.argv[2])
    except IndexError:
        station_name, freq = 'station0', 10

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.station['url']))
    channel = connection.channel()
    channel.queue_declare(queue=LOCAL_QUEUE, durable=True)
    station = Station(name=station_name)

    for i in range(5):  # Send only 5 messages for test
        message = {'name': station.name, 'temperature': station.read(), 'timestamp': int(time.time())}
        channel.basic_publish(
            exchange='',
            routing_key=LOCAL_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))

        print(" [x] Sent %r" % message)
        time.sleep(freq)

    connection.close()


def consume():
    # Prepare the local station connection
    local_connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.station['url']))
    local_channel = local_connection.channel()
    local_channel.queue_declare(queue=LOCAL_QUEUE, durable=True)

    print(' [*] Consumer is waiting for messages. To exit press CTRL+C')

    # Prepare the connection with the remote queue
    remote_connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.server['url']))
    remote_channel = remote_connection.channel()
    remote_channel.queue_declare(queue=REMOTE_QUEUE, durable=True)

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        print(" [x] Send message to the server %r" % body)
        remote_channel.basic_publish(
            exchange='',
            routing_key=REMOTE_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))

        # Only send the ack after sucessfuly send the message to the server
        ch.basic_ack(delivery_tag=method.delivery_tag)

    local_channel.basic_qos(prefetch_count=1)
    local_channel.basic_consume(queue=LOCAL_QUEUE, on_message_callback=callback)

    local_channel.start_consuming()


if __name__ == '__main__':
    executor = ProcessPoolExecutor(2)
    loop = asyncio.get_event_loop()
    producer = asyncio.ensure_future(loop.run_in_executor(executor, produce))
    consumer = asyncio.ensure_future(loop.run_in_executor(executor, consume))
    loop.run_forever()
