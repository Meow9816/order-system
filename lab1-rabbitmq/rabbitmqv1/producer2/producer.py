# Import the 'pika' library for RabbitMQ communication.
import pika
import time
# Parse the AMQP URL to extract connection parameters.
url_params = pika.URLParameters('amqp://rabbit_mq?connection_attempts=10&retry_delay=10')

# Establish a blocking connection to the RabbitMQ server.
connection = pika.BlockingConnection(url_params)

channel = connection.channel()

# Define the exchange name and type (in this case, "direct").
exchange_name = 'direct_logs2'
exchange_type = 'direct'

# Declare the exchange.
channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

# Define the queue name.
queue_name = 'my_queue2'

# Declare the queue.
channel.queue_declare(queue=queue_name)

# Bind the queue to the exchange with a specific routing key.
routing_key = 'info'  # This can be any key that matches the routing.
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

counter = 0
try:
    while True:
        counter += 1
        message = f'Hello, RabbitMQ! Message #{counter}'
        channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message)
        print(f"Sent: '{message}' with routing key '{routing_key}'")
        time.sleep(2)  
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    channel.close()
    connection.close()
