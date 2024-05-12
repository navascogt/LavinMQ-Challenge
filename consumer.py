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
channel.queue_declare(queue='hr_queue')
channel.queue_declare(queue='marketing_queue')
channel.queue_declare(queue='support_queue')


def callback_hr(ch, method, properrties, body):
    print(f'Recieved #{ body }')


channel.basic_consume(
    'hr_queue',
    callback_hr,
    auto_ack=True
)


def callback_marketing(ch, method, properrties, body):
    print(f'Recieved #{ body }')


channel.basic_consume(
    'marketing_queue',
    callback_marketing,
    auto_ack=True
)


def callback_support(ch, method, properrties, body):
    print(f'Recieved #{ body }')


channel.basic_consume(
    'support_queue',
    callback_support,
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
