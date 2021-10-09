import json
from pymongo import MongoClient
import logging
import my_library
from kafka import KafkaConsumer, KafkaProducer
from json import dumps

# Constants
MONGODB_HOST = 'mongodb://localhost:27017/?readPreference=primary&appname=BigDataProject&ssl=false'
MONGODB_DBNAME = 'BIG_DATA_PROJECT'
MONGODB_COLLECTION_NAME = "asset_data"

asset_topic = "asset_data"

# Initialize
assets = {}
last_price = {}
group_id = {}
direction = {}
for asset in my_library.get_assets():
    assets[asset["asset_id"]] = []
    last_price[asset["asset_id"]] = {}
    group_id[asset["asset_id"]] = 1
    direction[asset["asset_id"]] = "up"

# MongoDB init
connection = MongoClient(MONGODB_HOST)
if MONGODB_DBNAME not in connection.list_database_names():
    logging.warning("DB not Found")

db = connection[MONGODB_DBNAME]
collection_kripto = db[MONGODB_COLLECTION_NAME]

# Kafka init
consumer = KafkaConsumer(asset_topic, bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


def main():
    for message in consumer:
        key = message.key
        key = key.decode('utf-8')
        if key in assets:
            value = message.value
            assets[key].append(value)
            previous = last_price[key]
            if "updated_at" in previous:
                if value["updated_at"] > previous["updated_at"]:
                    print(key)
                    if value["price"] > previous["price"]:
                        if direction[key] != "up":
                            direction[key] = "up"
                            group_id[key] = group_id[key] + 1
                        print("fiyat arttı!")
                        # producer.send(asset_topic, value=data_0h, key=data_0h["asset_id"].encode('utf-8'))
                    elif value["price"] < previous["price"]:
                        if direction[key] != "down":
                            direction[key] = "down"
                            group_id[key] = group_id[key] + 1
                        print("fiyat düştü!")
                    print(previous["price"])
                    print(value["price"])
                    last_price[key] = value  # Güncellendi
            else:
                last_price[key] = value  # İlk kayıt

            value["group_id"] = group_id[key]
            if collection_kripto.count_documents({"asset_id": key, 'updated_at': value["updated_at"]}) == 0:
                collection_kripto.insert_one(value)
                print(".", end="")
    consumer.close()


if __name__ == '__main__':
    main()
