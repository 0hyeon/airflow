import os
import random
import csv


def generate_csv(filename, data):
    with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['product','label'])
        for d in data:
            writer.writerow(d)

def parse_dataset(path):
    """
    ['과자', '디저트', '면류', '미분류', '상온HMR', '생활용품', '소스', '유제품', '음료', '의약외품', '이_미용', '주류', '커피차', '통조림_안주', '홈클린']
    """
    label_dir = [i[:-4] for i in os.listdir(path) if '.zip' in i]
    labels = ['과자', '디저트', '면류', '미분류', '상온HMR', '생활용품', '소스', '유제품', '음료', '의약외품', '이_미용', '주류', '커피차', '통조림_안주', '홈클린']
    
    total_data = []
    for directory in label_dir:
        label = labels.index(directory[4:])

        label_path = f'{path}\\{directory}'
        data = [[''.join(i.split('_')[1:]), label] for i in os.listdir(label_path)]

        total_data = total_data + data

    random.shuffle(total_data)
    len_train = int(len(total_data) * 0.9)

    train_data = total_data[:len_train]
    test_data =total_data[len_train:]

    generate_csv('train.csv', train_data)
    generate_csv('test.csv', test_data)

parse_dataset('../../../data/Validation')