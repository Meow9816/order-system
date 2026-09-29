# Import the 'pika' library for RabbitMQ communication.
import pika
# Parse the AMQP URL to extract connection parameters.
url_params = pika.URLParameters('amqp://rabbit_mq?connection_attempts=10&retry_delay=10')

# Establish a blocking connection to the RabbitMQ server.
connection = pika.BlockingConnection(url_params)

channel = connection.channel()

# Define the exchange name and type (in this case, "direct").
exchange_name = 'headers_logs'
exchange_type = 'headers'

# Declare the exchange.
channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

# Define the queue name.
queue_name = 'my_queue'

# Declare the queue.
channel.queue_declare(queue=queue_name)

# Bind the queue to the exchange with a specific routing key.
#routing_key = 'info2'  # This can be any key that matches the routing.
bilding_arguments = {
    'x-match' : 'any',
    'header1' : 'value1'
}
channel.queue_bind(exchange=exchange_name, queue=queue_name, arguments=bilding_arguments)
headers = {
            'header1' : 'value1',
            'header2' : 'value2'
        }

message = f'Hello, RabbitMQ!'
channel.basic_publish(exchange=exchange_name, routing_key='', body=message, properties=pika.BasicProperties(headers=headers))
print(f"Sent: '{message}' with routing key")
       
channel.close()
connection.close()
