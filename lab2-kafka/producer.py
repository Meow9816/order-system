from confluent_kafka import Producer
import time

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка: {err}", flush=True)
    else:
        print(
            f"Отправлено: ключ {msg.key().decode('utf-8')} -> "
            f"Партиция {msg.partition()} offset {msg.offset()}",
            flush=True
        )

print("Продюсер отправляет сообщения с ключами (бесконечный цикл)...", flush=True)

topic = 'multi-partition-topic'
users = ['alice', 'bob', 'carol', 'dave']

i = 1
try:
    while True:                              # <-- бесконечный цикл
        user_id = users[i % len(users)]
        message = f"Hello Kafka #{i} for {user_id}"
        producer.produce(
            topic=topic,
            key=user_id.encode('utf-8'),
            value=message.encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
        i += 1
        time.sleep(0.5)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Продюсер завершил работу", flush=True)