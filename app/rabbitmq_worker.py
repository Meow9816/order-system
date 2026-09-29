import os, sys, time, pika
app_name = 'RabbitWorker'
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_URL = f'amqp://pi:pi@{RABBITMQ_HOST}:5672/%2F?connection_attempts=10&retry_delay=10'
RABBITMQ_EXCHANGE = 'order_exchange'
RABBITMQ_QUEUE = 'order-notifications'
RABBITMQ_ROUTING_KEY = 'order'

def connect():
    for attempt in range(10):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            print(f"[{app_name}] Подключение к RabbitMQ установлено", flush=True)
            return connection
        except Exception as e:
            print(f"[{app_name}] RabbitMQ не готов (попытка {attempt+1}/10): {e}", flush=True)
            time.sleep(3)
    raise RuntimeError("Не удалось подключиться к RabbitMQ")

def on_message(channel, method, properties, body):
    try:
        text = body.decode('utf-8')
    except UnicodeDecodeError:
        text = repr(body)
    print(f"[{app_name}] Получено уведомление: {text}", flush=True)
    channel.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = connect()
    channel = connection.channel()
    channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='direct', durable=True)
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key=RABBITMQ_ROUTING_KEY)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message)
    print(f"[{app_name}] Ожидаю сообщения в очереди '{RABBITMQ_QUEUE}'...", flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"[{app_name}] Фатальная ошибка: {e}", file=sys.stderr)
        sys.exit(1)
