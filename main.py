import csv
import re

from pymongo import MongoClient
import pymongo

import json
from collections import OrderedDict



# mongo_client = MongoClient()
# mongo_db = mongo_client[db]
# tickets_collection = mongo_db['tickets']


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)

        input_dict = list(reader)
        output_dict = json.loads(json.dumps(input_dict))

        mongo_client = MongoClient()
        mongo_db = mongo_client[db]
        tickets_collection = mongo_db['tickets']

        result = tickets_collection.insert_many(output_dict)

    return


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """ 

    mongo_client = MongoClient()
    mongo_db = mongo_client[db]
    tickets_collection = mongo_db['tickets']

    ordered_collection = mongo_db.tickets_collection.find().sort('Цена', pymongo.ASCENDING)

    return ordered_collection


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и вернуть их по возрастанию цены
    """

    regex = re.compile('укажите регулярное выражение для поиска. ' \
                       'Обратите внимание, что в строке могут быть специальные символы, их нужно экранировать')


if __name__ == '__main__':
    read_data('artists.csv', 'concerts-db')
    find_cheapest('concerts-db')


