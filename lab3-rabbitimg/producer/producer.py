import pika
import os
import time
import json

amqp_url = os.environ.get('RABBITMQ_URL', 'amqp://rabbit_mq?connection_attempts=10&retry_delay=10')
url_params = pika.URLParameters(amqp_url)
connection = pika.BlockingConnection(url_params)
channel = connection.channel()

exchange_name = 'image_topic'
channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

IMAGE_DIR = '/usr/src/app/producer/images'
SENT_LOG = '/usr/src/app/producer/sent_files.txt'

def get_sent_files():
    if os.path.exists(SENT_LOG):
        with open(SENT_LOG, 'r') as f:
            return set(f.read().splitlines())
    return set()

def mark_as_sent(filename):
    with open(SENT_LOG, 'a') as f:
        f.write(filename + '\n')

sent_files = get_sent_files()

print("Producer started. Watching folder:", IMAGE_DIR)

try:
    while True:
        if os.path.exists(IMAGE_DIR):
            files = [f for f in os.listdir(IMAGE_DIR) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
            for filename in files:
                if filename not in sent_files:
                    filepath = os.path.join(IMAGE_DIR, filename)
                    try:
                        with open(filepath, 'rb') as f:
                            image_bytes = f.read()
                        channel.basic_publish(
                            exchange=exchange_name,
                            routing_key='image.process',
                            body=image_bytes,
                            properties=pika.BasicProperties(
                                headers={'filename': filename}
                            )
                        )
                        print(f"Sent: {filename}")
                        mark_as_sent(filename)
                    except Exception as e:
                        print(f"Error sending {filename}: {e}")
        
        time.sleep(5) 

except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    channel.close()
    connection.close()