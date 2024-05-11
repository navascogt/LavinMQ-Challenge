import pika, os, sys
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

def callback(ch, method, properrties, body):
    print(f'Recieved #{ body }')

channel.basic_consume(
    'hello_world',
    callback,
    auto_ack=True
)

try:
    print('\n Waiting for mesage. To exit press CTRL+C \n')
    channel.start_consuming()
except Exception as e:
    print(f'Error: #{e}')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
