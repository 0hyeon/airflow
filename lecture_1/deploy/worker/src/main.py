import argparse
import requests
import json
import re

import numpy as np
from confluent_kafka import Consumer, Producer
from tensorflow.keras.preprocessing.text import tokenizer_from_json
from tensorflow.keras.preprocessing.sequence import pad_sequences
from konlpy.tag import Okt


def process(api_host, model_name, message, tokenizer, okt, max_len):
    url = f'http://{api_host}/v1/models/{model_name}:predict'

    text = message['input']
    text = re.sub(r'[^A-Za-z0-9가-힣]', ' ', text)
    tokens = okt.morphs(text)
    x = tokenizer.texts_to_sequences([tokens])
    x = pad_sequences(x, maxlen=max_len, padding='post').tolist()

    headers = {'Content-Type': 'application/json'}
    payload = {"instances": x}

    res = requests.post(url, headers=headers, data=json.dumps(payload)).json()
    idx = np.argmax(res['predictions'][0])

    labels = ['과자', '디저트', '면류', '미분류', '상온HMR', '생활용품', '소스', '유제품', '음료', '의약외품', '이_미용', '주류', '커피차', '통조림_안주', '홈클린']

    processed_message = {
        'id': message['id'],
        'output': labels[idx]
    }

    return processed_message

def print_assignment(consumer, partitions):
    print(f'Assignment: {partitions}')

def main(args):
    consumer_conf = {
        'bootstrap.servers': args.brokers,
        'group.id': args.group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.offset.store': True
    }

    producer_conf = {
        'bootstrap.servers': args.brokers,
        'client.id': args.group_id
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.topic_to_consume], on_assign=print_assignment)

    producer = Producer(producer_conf)

    okt=Okt()
    with open(args.tokenizer_path, 'r', encoding='utf-8') as f:
        tokenizer_data = json.load(f)
        tokenizer = tokenizer_from_json(tokenizer_data)

    while True:
        message = consumer.poll(timeout=1.0)
        if message:
            processed_message = process(
                args.api_host,
                args.model_name,
                json.loads(message.value()),
                tokenizer,
                okt,
                args.max_len
            )

            json_msg = json.dumps(processed_message, ensure_ascii=False)
            producer.produce(
                args.topic_to_produce,
                key=bytes(processed_message['id'], encoding='utf-8'),
                value=bytes(json_msg, encoding='utf-8'))
            producer.poll(0.1)
        else:
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic_to_consume', type=str, default='category-match-in')
    parser.add_argument('--topic_to_produce', type=str, default='category-match-out')

    parser.add_argument('--brokers', type=str, default='kafka-service:9092')
    parser.add_argument('--group_id', type=str, default='TEST')

    parser.add_argument('--api_host', type=str, default='category-match.default.svc.cluster.local')
    parser.add_argument('--model_name', type=str, default='category-match')

    parser.add_argument('--tokenizer_path', type=str, default='/data/tokenizer/tokenizer.json')
    parser.add_argument('--max_len', type=int, default=20)

    args = parser.parse_args()

    main(args)
