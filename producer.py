import pika, os
from dotenv import load_dotenv

load_dotenv()

# Access the CLOUADAMQP_URL
url = os.environ.get('CLOUDAMQP_URL')

# Create a connection
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)

# Create a Channel
channel = connection.channel()
print("Channel over a connection created")

# Declare a queue
channel.queue_declare(queue='hello_world')


def send_to_queue(channel, routing_key, body):
    channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=body
    )
    print(f"Message sent to queue - msg: #{body}")


send_to_queue(
    channel=channel, routing_key='hello_world', body='Hello World'
)

send_to_queue(
    channel=channel, routing_key='hello_world', body='Hello World'
)

send_to_queue(
    channel=channel, routing_key='hello_world', body='Hello World'
)

try:
    connection.close()
    print('Connection Closed')
except Exception as e:
    print(f'Errort: #{e}')