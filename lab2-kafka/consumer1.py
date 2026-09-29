import os
from confluent_kafka import Consumer, KafkaError

worker = os.getenv('WORKER_NAME', 'Worker')
topic = 'multi-partition-topic'

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'group-1',          # <-- первая группа
    'auto.offset.reset': 'earliest'
}

def on_assign(consumer, partitions):
    ids = sorted(p.partition for p in partitions)
    print(f"{worker} [group-1] Ребалансировка: назначены партиции {ids}", flush=True)

def on_revoke(consumer, partitions):
    ids = sorted(p.partition for p in partitions)
    print(f"{worker} [group-1] отобраны партиции {ids}", flush=True)

consumer = Consumer(conf)
consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

print(f"{worker} [group-1] - Консьюмер запущен, ожидает сообщений...", flush=True)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                continue
            print(f"Ошибка: {msg.error()}", flush=True)
            break

        key = msg.key().decode('utf-8') if msg.key() else None
        message = msg.value().decode('utf-8')
        print(
            f"{worker} [group-1] Получено: {message} | "
            f"Партиция: {msg.partition()} | offset: {msg.offset()} | ключ: {key}",
            flush=True
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()