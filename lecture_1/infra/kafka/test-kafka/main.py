import time
from confluent_kafka import Producer, Consumer

conf = {'bootstrap.servers': 'kafka-service:9092',
        'client.id': 'TEST'}

topic = 'category-match-out'

producer = Producer(conf)
producer.produce(topic, key="test-key", value="test-value")
producer.poll(1)

conf = {'bootstrap.servers': 'kafka-service:9092',
        'group.id': 'TEST',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'true'}

consumer = Consumer(conf)
consumer.subscribe([topic])
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is not None:
        print(msg.value())

    time.sleep(3)

consumer.close()