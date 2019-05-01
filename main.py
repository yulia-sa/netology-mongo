import csv
import re
from pprint import pprint

import pymongo
# from pymongo import MongoClient

import json


mongo_client = pymongo.MongoClient()


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    mongo_db = mongo_client[db]
    tickets_collection = mongo_db['tickets']

    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)

        input_dict = list(reader)
        output_dict = json.loads(json.dumps(input_dict))

        mongo_db = mongo_client[db]

        tickets_collection.insert_many(output_dict)

    return tickets_collection


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """ 
    mongo_db = mongo_client[db]

    ordered_collection = mongo_db.tickets_collection.find().sort('Цена', pymongo.ASCENDING)

    return ordered_collection


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и вернуть их по возрастанию цены
    """
    mongo_db = mongo_client[db]

    regex = re.compile('[\S ]*.*' + str(name) + '[\S ]*.*')
    tickets_list = list(tickets_collection.find({'Исполнитель': {'$regex': regex, '$options': '-i'}}))
    tickets_list_sorted = sorted(tickets_list, key=lambda k: int(k['Цена']))

    return tickets_list_sorted


if __name__ == '__main__':
    tickets_collection = read_data('artists.csv', 'concerts-db')
    pprint(list(tickets_collection.find()))

    tickets_by_price = find_cheapest('concerts-db')
    print(tickets_by_price)

    tickets_list_sorted = find_by_name('а', 'concerts-db')
    pprint(tickets_list_sorted)
