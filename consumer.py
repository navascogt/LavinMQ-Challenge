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
print('Channel over a connection created')

# List group container
user_groups = ['hr', 'marketing', 'support']

# Get user group argument from commandline
user_group = sys.argv[1]

# Validate for user group
if not user_group:
    sys.stderr.write('Usage: %s [hr] [marketing] [support]\n' % sys.argv[0])
    sys.exit(1)

if user_group not in user_groups:
    sys.stderr.write('Invalid argument - allowed arguments: %s [hr] [marketing] [support]\n' % sys.argv[0])
    sys.exit(1)

# Declare a queue
queue_name = user_group + '_queue'
query_binding_key = user_group

# Declare a exchange, queue and bindings
channel.exchange_declare(
    exchange='slack_notifications',
    exchange_type='direct'
)

channel.queue_declare(
    queue=queue_name
)

channel.queue_bind(
    exchange='slack_notifications',
    queue=queue_name,
    routing_key=query_binding_key
)


def callback(ch, method, properties, body):
    print(f'Recieved #{ body }')
    ch.basic_ack(delivery_tag= method.delivery_tag)


channel.basic_consume(
    queue_name,
    callback
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
