import os
import json

import numpy as np

import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer, tokenizer_from_json
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.layers import Embedding, Dense, LSTM
from tensorflow.keras.models import Sequential
from tensorflow.keras.models import load_model

import pandas as pd
from konlpy.tag import Okt

def get_tokenizer(filename, tokens):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            tokenizer = tokenizer_from_json(data)
    else:
        tokenizer = Tokenizer()
        tokenizer.fit_on_texts(tokens)
        tokenizer_json = tokenizer.to_json()
        with open('tokenizer.json', 'w', encoding='utf-8') as f:
            f.write(json.dumps(tokenizer_json, ensure_ascii=False))

    return tokenizer

def preprocess_1(data):
    okt=Okt()
    data['product'] = data['product'].str.replace('[^A-Za-z0-9가-힣]', ' ', regex=True)
    data['tokenized'] = data['product'].apply(okt.morphs)

    return data

def preprocess_2(data, tokenizer, max_len):
    x = tokenizer.texts_to_sequences(data['tokenized'])
    x = pad_sequences(x, maxlen=max_len, padding='post')
    y = data['label'].tolist()

    return x, y

def get_model(vocab_size, num_label):
    model = Sequential()
    model.add(Embedding(vocab_size, 64))
    model.add(LSTM(256))
    model.add(Dense(num_label, activation='softmax'))

    print(model.summary())

    return model

def main():
    max_len = 20
    num_label= 15

    train_data = pd.read_csv('train.csv', header=0)
    test_data = pd.read_csv('test.csv', header=0)

    train_data = preprocess_1(train_data)
    test_data = preprocess_1(test_data)

    tokenizer = get_tokenizer('tokenizer.json', train_data['tokenized'])

    vocab_size = len(tokenizer.word_index)+1

    train_x, train_y = preprocess_2(train_data, tokenizer, max_len=max_len)
    text_x, test_y = preprocess_2(test_data, tokenizer, max_len=max_len)

    model = get_model(vocab_size, num_label)
    model.compile(optimizer='rmsprop', loss='sparse_categorical_crossentropy', metrics=['acc'])
    history = model.fit(np.array(train_x), np.array(train_y), epochs=5, batch_size=64)

    tf.saved_model.save(model, 'model-store/model')

main()
