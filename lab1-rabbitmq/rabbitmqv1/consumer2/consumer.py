# Import the 'pika' library for RabbitMQ communication.
import pika

# Parse the AMQP URL to extract connection parameters.
url_params = pika.URLParameters('amqp://rabbit_mq?connection_attempts=10&retry_delay=10')

# Establish a blocking connection to the RabbitMQ server.
connection = pika.BlockingConnection(url_params)

channel = connection.channel()

# Define the queue name.
queue_name = 'my_queue2'

# Declare the queue.
channel.queue_declare(queue=queue_name)

# Function to process received messages.
def callback(ch, method, properties, body):
    print(f"Received message: {body}")
    # You can add your processing logic here

def consume():
    # Set the callback function to process messages
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')

    # Start consuming messages from the queue
    channel.start_consuming()

consume()

# Close the channel.
channel.close()

# Close the connection to RabbitMQ.
connection.close()