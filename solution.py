"""
Задание:
Обработка транзакций покупок и создание отчета.
Данные о транзакциях представлены в виде файла json, в котором есть структуры, где у каждой транзакции есть:
- свой уникальный id этой транзакции
- уникальный id пользователя
- уникальный id продукта, который купил пользователь
- price usd - цена, за которую пользователь купил этот продукт

Задача: загрузить данные, обработать, создать отчет
А также дедублицировать данные по уникальному id этой транзакции, отчет представить в формате csv
"""


import csv
import argparse
from dataclasses import dataclass

import numpy as np
from dataclasses_json import dataclass_json
import pandas as pd
from numpy.ma.extras import unique


@dataclass_json
@dataclass
class Transaction:
    id: str
    user_id: str
    product_id: str
    price_usd: float

@dataclass_json
@dataclass
class TransactionList:
    transactions: list[Transaction]


def main(data_path, report_path):
    with open(data_path, 'r') as f:
        t_list: TransactionList = TransactionList.schema().loads(f.read())
        print(len(t_list.transactions))
        #print(t_list.transactions[0])

        #Получаем словарь с уникальными транзакциями,
        # где ключ - уникальная транзакция,
        # значение - класс Transaction(id: str, user_id: str, product_id: str, price_usd: float(.2))
        unique_transactions = {}

        for t in t_list.transactions:
            t_id = t.id
            u_id = t.user_id
            if t_id not in unique_transactions:
                unique_transactions[t_id] = t



        #print(len(unique_transactions)) #проверяем длину словаря с уникальными транзакциями, сравнивая с неуникальными
        #print(unique_transactions['87133067-063c-47b3-997f-cae750b70347']) #сверяем данные по конкретной транзакции

        #Создаем словарь с ценами для каждого уникального пользователя,
        # где ключ - уникальный пользователь: str
        # значение - список цен всех покупок, которые он совершал
        dict_price_usd = {}

        # Создаем словарь с id продуктов для каждого уникального пользователя,
        # где ключ - уникальный пользователь: str
        # значение - словарь всех продуктов, которые он покупал в формате 'product_id':количество приобретений
        dict_product_id = {}

        #Цикл для заполнения словарей
        for t in unique_transactions.values():
            u_id = t.user_id        #user_id  для dict_price_usd
            p_usd = t.price_usd     #price_id для dict_price_usd
            #pr_id = t.product_id    #product_id для dict_product_id
            if u_id not in dict_price_usd.keys():
                #user_id.append(u_id)
                dict_price_usd[u_id] = []
                dict_price_usd[u_id].append(p_usd)
            if u_id not in dict_product_id:
                dict_product_id[u_id] = dict()
                dict_product_id[u_id][t.product_id] = 1
            else:
                dict_price_usd[u_id].append(p_usd)
                if t.product_id not in dict_product_id[u_id].keys():
                    dict_product_id[u_id][t.product_id] = 1
                else:
                    dict_product_id[u_id][t.product_id] = dict_product_id[u_id][t.product_id] + 1

        #print(dict_product_id['02f498ef-cffa-45de-acc9-c7534be066e4'])
        #Проверка (убрать)
        #print(dict_product_id['b02fdb0c-798e-4bc2-99ae-508da9cc9b25'])

        #Забираем всех уникальных пользователей (они же ключи словаря) в список
        user_id_keys = list(dict_price_usd.keys())

        #print(user_id_keys) проверка, убрать
        #print(len(user_id_keys)) проверка, убрать
        #print(dict_price_usd['b02fdb0c-798e-4bc2-99ae-508da9cc9b25']) проверка, убрать
        #print(dict_price_usd) проверка, убрать
        #print(user_id_keys.index('fbad5ca7-da41-45b3-aac2-44026e5f2d83')) проверка, убрать

        #Создаем новые списки для DataFrame
        min_price_usd = []
        max_price_usd = []
        avg_price_usd = []
        for u_id in dict_price_usd:
            min_price_usd.append(min(dict_price_usd[u_id]))
            max_price_usd.append(max(dict_price_usd[u_id]))
            avg_price_usd.append(float(np.round(np.mean(dict_price_usd[u_id]), 2)))
        #print(min_price_usd[56])
        #print(max_price_usd[56])
        #print(avg_price_usd[56])

        #print(dict_product_id['ab26a833-1c78-47c5-9af1-b69792047f94'])
        #item = list(dict_product_id['ab26a833-1c78-47c5-9af1-b69792047f94'].items())
        #print(item[0][1])
        #print(dict_product_id.items())
        best_product_id = {}
        count = 0
        for u_id, inner in dict_product_id.items():
            for key, value in list(inner.items()):
                if u_id not in best_product_id:
                    best_product_id[u_id] = key
                    count = value
                else:
                    if value > count:
                        best_product_id[u_id] = key
                        count = value
                    #print(count)
        print(best_product_id['02f498ef-cffa-45de-acc9-c7534be066e4'])
        #print(best_product_id)
        #print(len(best_product_id))


        best_product_id = list(best_product_id.values())
        #print(best_product_id)

        df = pd.DataFrame({
            'user_id': user_id_keys,
            'min_price_usd': min_price_usd,
            'max_price_usd': max_price_usd,
            'avg_price_usd': avg_price_usd,
            'best_product_id': best_product_id,
        })
        #print(df)
        df.to_csv("report.csv", index=False)
        new_df = pd.read_csv("report.csv")
        print(new_df)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='transactions.json', help='Please set datasets path.')
    parser.add_argument('--report_path', type=str, default='report.csv', help='Please set report path.')
    args = parser.parse_args()
    data_path = args.data_path
    report_path = args.report_path
    main(data_path, report_path)

