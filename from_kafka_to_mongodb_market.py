import json
from pymongo import MongoClient
import my_library
from kafka import KafkaConsumer, KafkaProducer
from json import dumps

# Constants
MONGODB_HOST = 'mongodb://localhost:27017/?readPreference=primary&appname=BigDataProject&ssl=false'
MONGODB_DBNAME = 'BIG_DATA_PROJECT'
MONGODB_COLLECTION_NAME = "market_data"

market_topic = "market_data"
ui_alert_topic = "ui_alert"

# Initialize
assets = {}
last_price = {}
for asset in my_library.get_assets():
    assets[asset["asset_id"]] = []
    last_price[asset["asset_id"]] = {}

# MongoDB init
connection = MongoClient(MONGODB_HOST)
if MONGODB_DBNAME not in connection.list_database_names():
    print("DB not Found")

db = connection[MONGODB_DBNAME]
collection_market = db[MONGODB_COLLECTION_NAME]

# Kafka init
consumer = KafkaConsumer(market_topic, bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


def main():
    alarms = my_library.get_alarms_from_db()
    for message in consumer:
        key = message.key
        key = key.decode('utf-8')
        value = message.value
        base_asset = value["base_asset"]
        if base_asset in assets:
            quote_asset = value["quote_asset"]
            query = {"base_asset": base_asset, "quote_asset": quote_asset, 'updated_at': value["updated_at"]}
            if collection_market.count_documents(query) == 0:
                collection_market.insert_one(value)
                print(value)
                if "USD" in quote_asset and base_asset in alarms:
                    print("alert")
                    asset_alarm_list = alarms[base_asset]
                    for alarm in asset_alarm_list:
                        if value[alarm["market"] + "_price"] > alarm["target_price"]:
                            data = {"alert": {"asset_id": base_asset, "market": alarm["market"],
                                              "price": value[alarm["market"] + "_price"]}}
                            producer.send(ui_alert_topic, value=data, key=base_asset.encode('utf-8'))
                            print("publish to ui")
        else:
            print("Not found in")
    consumer.close()


if __name__ == '__main__':
    main()
