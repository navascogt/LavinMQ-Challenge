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

# Declare Exchange
channel.exchange_declare('slack_notifications', 'direct')

# Declare queues
channel.queue_declare(queue='hr_queue')
channel.queue_declare(queue='marketing_queue')
channel.queue_declare(queue='support_queue')

# Bind queues with exchange
channel.queue_bind('hr_queue', 'slack_notifications', 'hr')
channel.queue_bind('marketing_queue', 'slack_notifications', 'marketing')
channel.queue_bind('support_queue', 'slack_notifications', 'support')


def send_to_queue(channel, exchange, routing_key, body):
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        )
    )
    print(f'Message sent to queue - msg: #{body}')



while True:
    available_queues = ['hr', 'marketing', 'support', 'all', 'exit']

    queue_name = input('What queue do you want to send message: [hr] [marketing] [support] [all] [exit]\n\n')

    match queue_name:
        case 'exit':
            sys.exit(0)
        case queue_name if queue_name in ['hr', 'marketing', 'support']:
            send_to_queue(
                channel=channel, exchange='slack_notifications', routing_key=queue_name, body=queue_name.upper() + ' Slack Notification'
            )
        case 'all':
            queues = ['hr', 'marketing', 'support']
            for queue in queues:
                send_to_queue(
                    channel=channel, exchange='slack_notifications', routing_key=queue, body=queue.upper() + ' Slack Notification'
                )


send_to_queue(
    channel=channel, exchange="slack_notifications" ,routing_key='hr', body='HR Slack Notification'
)

send_to_queue(
    channel=channel, exchange='slack_notifications' ,routing_key='marketing', body='Marketing Slack Notification'
)

send_to_queue(
    channel=channel, exchange='slack_notifications' ,routing_key='support', body='Support Slack Notification'
)

try:
    connection.close()
    print('Connection Closed')
except Exception as e:
    print(f'Errort: #{e}')