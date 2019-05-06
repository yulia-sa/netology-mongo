import csv
import re
import pymongo
import json
from pprint import pprint

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

        for d in output_dict:
            d['Цена'] = int(d['Цена'])

        tickets_collection.insert_many(output_dict)

    return tickets_collection


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """ 
    mongo_db = mongo_client[db]
    tickets_collection = mongo_db['tickets']

    ordered_collection = tickets_collection.find().sort('Цена', pymongo.ASCENDING)

    return ordered_collection


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и вернуть их по возрастанию цены
    """
    mongo_db = mongo_client[db]
    tickets_collection = mongo_db['tickets']

    regex = re.compile('[\S ]*.*' + str(name) + '[\S ]*.*')
    tickets_sorted = tickets_collection.find(
                        {'Исполнитель': {'$regex': regex, '$options': '-i'}}
                        ).sort('Цена', pymongo.ASCENDING)

    return tickets_sorted


if __name__ == '__main__':
    tickets_collection = read_data('artists.csv', 'concerts-db')
    pprint(list(tickets_collection.find()))

    print('*' * 20)

    tickets_by_price = find_cheapest('concerts-db')
    pprint(list(tickets_by_price))

    print('*' * 20)

    tickets_sorted = find_by_name('а', 'concerts-db')
    pprint(list(tickets_sorted))
