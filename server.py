import pika
import config

REMOTE_QUEUE = 'message_queue'

connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.server['url']))
channel = connection.channel()

channel.queue_declare(queue=REMOTE_QUEUE, durable=True)
print(' [*] Server waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=REMOTE_QUEUE, on_message_callback=callback)

channel.start_consuming()