import json
from kafka import KafkaProducer
from json import dumps
from time import sleep
import my_library
from datetime import datetime, timedelta
import copy
from kafka.admin import KafkaAdminClient, NewTopic
import time

# Initialize
refresh_duration_in_seconds = 20
asset_topic = "asset_data"
market_topic = "market_data"
ui_alert_topic = "ui_alert"

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


def create_kafka_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id='test'
    )

    my_topic_list = [asset_topic, market_topic, ui_alert_topic]

    # Check existing topics
    topics_from_kafka = admin_client.list_topics()
    new_topic_list = []
    for topic in my_topic_list:
        if topic not in topics_from_kafka:
            # If not exist then add to create list
            new_topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

    # create non-existing topics
    if len(new_topic_list) > 0:
        admin_client.create_topics(new_topics=new_topic_list, validate_only=False)


def main():
    start_time = time.time()
    create_kafka_topics()
    my_library.check_exchanges()
    half_hour = 30 * 60
    twelve_hour = 12 * 60 * 60
    only_for_my_assets = True

    assets = []
    if only_for_my_assets:
        for asset in my_library.get_my_assets_from_db():
            assets.append({"asset_id": asset["currency"], 'updated_at': "new", 'market_updated_at': "new"})
    else:
        for asset in my_library.get_assets():
            assets.append({"asset_id": asset["asset_id"], 'updated_at': "new", 'market_updated_at': "new"})
    while True:
        elapsed = time.time() - start_time

        if only_for_my_assets:
            for asset in my_library.get_my_assets_from_db():
                found = any(asset["currency"] in asset_for_loop["asset_id"] for asset_for_loop in assets)
                if not found:
                    assets.append({"asset_id": asset["currency"], 'updated_at': "new", 'market_updated_at': "new"})
                    print(asset["currency"], " added!")

        asset_data = my_library.get_assets()
        for data_0h in asset_data:
            for my_asset in assets:
                if my_asset["asset_id"] == data_0h["asset_id"]:
                    print("asset ", data_0h["asset_id"], end="")

                    updated_at = data_0h['updated_at']
                    last_updated_at = my_asset['updated_at']
                    if updated_at != last_updated_at:
                        my_asset['updated_at'] = updated_at

                        del(data_0h['volume_24h'])
                        del(data_0h['status'])
                        del(data_0h['created_at'])

                        change_1h = data_0h['change_1h']
                        del(data_0h['change_1h'])

                        change_24h = data_0h['change_24h']
                        del(data_0h['change_24h'])

                        change_7d = data_0h['change_7d']
                        del(data_0h['change_7d'])

                        # print(data)
                        producer.send(asset_topic, value=data_0h, key=data_0h["asset_id"].encode('utf-8'))

                        try:
                            updated_at = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%S.%f")
                        except ValueError:
                            print("ValueError:", updated_at)
                            updated_at = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%S")

                        if elapsed < half_hour:
                            data_1h_ago = copy.deepcopy(data_0h)
                            updated_at_1h_ago = updated_at - timedelta(hours=1.)
                            data_1h_ago["updated_at"] = str(updated_at_1h_ago).replace(" ", "T")
                            data_1h_ago["price"] = data_1h_ago["price"] + change_1h
                            # print(data_1h_ago)
                            producer.send(asset_topic, value=data_1h_ago, key=data_1h_ago["asset_id"].encode('utf-8'))

                        if elapsed < twelve_hour:
                            data_24h_ago = copy.deepcopy(data_0h)
                            updated_at_24h_ago = updated_at - timedelta(hours=24.)
                            data_24h_ago["updated_at"] = str(updated_at_24h_ago).replace(" ", "T")
                            data_24h_ago["price"] = data_24h_ago["price"] + change_24h
                            # print(data_24h_ago)
                            producer.send(asset_topic, value=data_24h_ago, key=data_24h_ago["asset_id"].encode('utf-8'))

                        # no need to get this for project
                        if False:
                            data_7d_ago = copy.deepcopy(data_0h)
                            updated_at_7d_ago = updated_at - timedelta(days=7.)
                            data_7d_ago["updated_at"] = str(updated_at_7d_ago).replace(" ", "T")
                            data_7d_ago["price"] = data_7d_ago["price"] + change_7d
                            # print(data_7d_ago)
                            producer.send(asset_topic, value=data_7d_ago, key=data_7d_ago["asset_id"].encode('utf-8'))
                    print(".")

        markets_df = my_library.get_asset_market_price()
        market_data = markets_df.toJSON().collect()
        for data in market_data:
            data = json.loads(data)
            market_updated_at = data['updated_at']
            base_asset = data["base_asset"]
            quote_asset = data["quote_asset"]
            for my_asset in assets:
                asset_id = my_asset['asset_id']
                if asset_id == base_asset:
                    # if only_for_my_assets:
                    #     found = any(quote_asset in asset_for_loop["asset_id"] for asset_for_loop in assets)
                    #     if not found:
                    #         continue
                    print("market ", asset_id, " ", end="")
                    market_last_updated_at = my_asset['market_updated_at']
                    if market_updated_at != market_last_updated_at:
                        my_asset['market_updated_at'] = market_updated_at
                        # print(data)
                        key = base_asset + quote_asset
                        producer.send(market_topic, value=data, key=key.encode('utf-8'))
                    else:
                        print("no change!")
                    print(".")

        print("Waiting...")
        sleep(refresh_duration_in_seconds)


if __name__ == '__main__':
    main()
