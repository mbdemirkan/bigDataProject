import requests
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pandas as pd
from pyspark.sql.functions import when, col, lit
from pyspark.sql import functions as F
from pymongo import MongoClient
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

sc = SparkContext.getOrCreate()
spark = SparkSession.Builder().appName("Kripto Proje").getOrCreate()


#
# MongoDB init
#
MONGODB_HOST = 'mongodb://localhost:27017/?readPreference=primary&appname=BigDataProject&ssl=false'
MONGODB_DBNAME = 'BIG_DATA_PROJECT'
FROM_CRYPTINGUP_TOPIC = "bigdata"

# MongoDB init
connection = MongoClient(MONGODB_HOST)


#
# Functions
#
def get_json_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return {}


def get_asset(asset):
    json_object = get_json_from_api("https://www.cryptingup.com/api/assets/" + asset)
    return json_object["asset"]


def get_assets():
    json_object = get_json_from_api("https://www.cryptingup.com/api/assets")
    assets = []
    assets = assets + json_object["assets"]
    while json_object["next"] != "":
        json_object = get_json_from_api("https://www.cryptingup.com/api/assets?start=" + json_object["next"])
        assets = assets + json_object["assets"]

    return assets


def get_asset_markets(asset):
    json_object = get_json_from_api("https://www.cryptingup.com/api/assets/" + asset + "/markets")
    markets = []
    markets = markets + json_object["markets"]
    while json_object["next"] != "":
        json_object = get_json_from_api(
            "https://www.cryptingup.com/api/assets/" + asset + "/markets?start=" + json_object["next"])
        markets = markets + json_object["markets"]
    return markets


def get_markets():
    json_object = get_json_from_api("https://www.cryptingup.com/api/markets")
    markets = []
    markets = markets + json_object["markets"]
    while json_object["next"] != "":
        json_object = get_json_from_api("https://www.cryptingup.com/api/markets?start=" + json_object["next"])
        markets = markets + json_object["markets"]
        print(".", end="")
    return markets


def convert_to_dataframe(json_object):
    pdf = pd.json_normalize(json_object)

    # pdf.to_csv(r'C:\tmp\tmp.csv', index=False)
    # print("Saved!")

    local_df = spark.createDataFrame(pdf)
    return local_df


def get_first_five_in_volume_24h(assets_df):
    volume_24h_df = spark.sql("select * from assets order by volume_24h desc limit 5")
    data = "<table border=1><tr><th>Cryptocurrency</th><th>Volume Change in 24h</th></tr>"
    for asset in volume_24h_df.collect():
        data = data + "<tr><td>" + asset["asset_id"] + "</td><td>" + str(asset["volume_24h"]) + "</td></tr>"
    data = data + "</table>"
    return data


def get_last_five_in_volume_24h(assets_df):
    volume_24h_df = spark.sql("select * from assets order by volume_24h asc limit 5")
    data = "<table border=1><tr><th>Cryptocurrency</th><th>Volume Change in 24h</th></tr>"
    for asset in volume_24h_df.collect():
        data = data + "<tr><td>" + asset["asset_id"] + "</td><td>" + str(asset["volume_24h"]) + "</td></tr>"
    data = data + "</table>"
    return data


def get_first_five_in_change_1h(assets_df):
    volume_24h_df = spark.sql("select * from assets order by change_1h desc limit 5")
    data = "<table border=1><tr><th>Cryptocurrency</th><th>Change in 1h</th></tr>"
    for asset in volume_24h_df.collect():
        data = data + "<tr><td>" + asset["asset_id"] + "</td><td>" + str(asset["change_1h"]) + "</td></tr>"
    data = data + "</table>"
    return data


def get_last_five_in_change_1h(assets_df):
    volume_24h_df = spark.sql("select * from assets order by change_1h asc limit 5")
    data = "<table border=1><tr><th>Cryptocurrency</th><th>Change in 1h</th></tr>"
    for asset in volume_24h_df.collect():
        data = data + "<tr><td>" + asset["asset_id"] + "</td><td>" + str(asset["change_1h"]) + "</td></tr>"
    data = data + "</table>"
    return data


def create_asset_report_from_api():
    report = {}
    assets = get_assets()
    assets_df = convert_to_dataframe(assets)
    assets_df.createOrReplaceTempView('assets')
    report["first_five_in_volume_24h"] = get_first_five_in_volume_24h(assets_df)
    report["last_five_in_volume_24h"] = get_last_five_in_volume_24h(assets_df)
    report["first_five_in_change_1h"] = get_first_five_in_change_1h(assets_df)
    report["last_five_in_change_1h"] = get_last_five_in_change_1h(assets_df)
    # change_24h | change_7d
    return report


def \
        create_asset_report_from_db():
    in_last_1h = drop_ten_percent_in_last_1h()
    first_row = in_last_1h.first()
    in_last_1h = f"{first_row['asset_id']} varlığı {first_row['first']}'de başlayan %{first_row['percent']} oranında kayıp yaşamıştır."

    in_last_24h = drop_ten_percent_in_last_24h()
    first_row = in_last_24h.first()
    in_last_24h = f"{first_row['asset_id']} varlığı {first_row['first']}'de başlayan %{first_row['percent']} oranında kayıp yaşamıştır."

    highest_drop = highest_drop_in_last_24h()
    highest_drop_asset = f"{highest_drop['asset_id']}"
    highest_drop = f"{highest_drop['asset_id']} varlığı {highest_drop['first']}'de başlayan %{highest_drop['percent']} oranında kayıp yaşamıştır."

    wave_24h = wave_in_last_24h()
    wave_24h = f"{wave_24h['asset_id']} varlığı {wave_24h['first']}'de başlayan %{wave_24h['percent']} oranında kayıp yaşamıştır."

    highest_increase = highest_increase_in_last_24h()
    highest_increase_asset = f"{highest_increase['asset_id']}"
    # highest_increase = f"{highest_increase['asset_id']} varlığı {highest_increase['first']}'de başlayan %{highest_increase['percent']} oranında kazanç yaşamıştır."

    report = {"drop_ten_percent_in_last_1h": in_last_1h,
              "drop_ten_percent_in_last_24h": in_last_24h,
              "highest_drop_in_last_24h": highest_drop,
              "wave_in_last_24h": wave_24h,
              "highest_drop_asset": highest_drop_asset,
              "highest_increase_asset": highest_increase_asset}
    return report


def get_asset_markets_by_base(markets_df, asset):
    volume_24h_df = spark.sql("select * from markets where base_asset='" + asset + "'")
    volume_24h_df.show()
    print(volume_24h_df.count())
    data = "<table border=1><tr><th>Cryptocurrency</th><th>Volume Change in 24h</th></tr>"
    # for asset in volume_24h_df.collect():
    #    data = data + "<tr><td>" + asset["asset_id"] + "</td><td>" + str(asset["volume_24h"]) + "</td></tr>"
    # data = data + "</table>"
    return data


def get_asset_markets_by_quote(markets_df, asset):
    volume_24h_df = spark.sql("select * from markets where quote_asset='" + asset + "'")
    volume_24h_df.show()
    print(volume_24h_df.count())
    data = "<table border=1><tr><th>Cryptocurrency</th><th>Volume Change in 24h</th></tr>"
    # for asset in volume_24h_df.collect():
    #    data = data + "<tr><td>" + asset["asset_id"] + "</td><td>" + str(asset["volume_24h"]) + "</td></tr>"
    # data = data + "</table>"
    return data


def get_asset_markets_by_base_and_quote(markets_df, base, quote):
    volume_24h_df = spark.sql("select * from markets where base_asset = '" + base + "' and quote_asset='" + quote + "'")
    volume_24h_df.show()
    print(volume_24h_df.count())
    data = "<table border=1><tr><th>Cryptocurrency</th><th>Volume Change in 24h</th></tr>"
    # for asset in volume_24h_df.collect():
    #    data = data + "<tr><td>" + asset["asset_id"] + "</td><td>" + str(asset["volume_24h"]) + "</td></tr>"
    # data = data + "</table>"
    return data


def get_market_prices(exchanges_df):
    exchanges = exchanges_df.select("exchange_id").rdd.map(lambda x: x[0]).collect()
    for exchange_id in exchanges:
        result = spark.sql(
            "select m.base_asset, m.quote_asset, m.price_unconverted as " + exchange_id + " from markets m, exchanges e where m.exchange_id = e.exchange_id and e.exchange_id = '" + exchange_id + "'")
    result.show()
    exit()
    return result


def get_exchanges():
    json_object = get_json_from_api("https://www.cryptingup.com/api/exchanges")
    return json_object["exchanges"]


def process_market_data(markets_df, exchanges):
    sql = "SELECT base_asset, quote_asset, max(updated_at) updated_at"

    for exchange in exchanges:
        exchange_id = exchange["exchange_id"]
        exchange_price = exchange_id + "_price"
        sql = sql + ",SUM(" + exchange_price + ") " + exchange_price

    sql = sql + " from markets group by base_asset, quote_asset"

    result = spark.sql(sql)

    return result


def get_asset_market_price():
    exchanges = get_exchanges()
    # markets = get_asset_markets(asset)
    markets = get_markets()
    markets_df = convert_to_dataframe(markets)

    exchanges_df = convert_to_dataframe(exchanges)
    exchanges_df.createOrReplaceTempView('exchanges')

    # base_markets = get_asset_markets_by_base(markets_df, asset)
    # quote_markets = get_asset_markets_by_quote(markets_df, asset)
    # get_asset_markets_by_base_and_quote(markets_df, asset, asset2)
    for exchange in exchanges:
        exchange_id = exchange["exchange_id"]
        exchange_price = exchange_id + "_price"

        markets_df = markets_df.withColumn(exchange_price,
                                           when(F.col("exchange_id") == exchange_id,
                                                F.col("price")).otherwise(0))
    markets_df = markets_df.drop("exchange_id")
    markets_df = markets_df.drop("symbol")
    markets_df = markets_df.drop("price_unconverted")
    markets_df = markets_df.drop("price")
    markets_df = markets_df.drop("change_24h")
    markets_df = markets_df.drop("spread")
    markets_df = markets_df.drop("volume_24h")
    markets_df = markets_df.drop("status")
    markets_df = markets_df.drop("created_at")
    markets_df.createOrReplaceTempView('markets')
    result = process_market_data(markets_df, exchanges)

    return result


def get_currency_rate(markets_df, base_asset, quote_asset):
    markets_df.createOrReplaceTempView('markets')
    sql = "select * from markets where base_asset='" + base_asset + "' and quote_asset='" + quote_asset + "'"
    currency_rate = spark.sql(sql)
    if currency_rate.count() == 0:
        sql = "select base_asset, quote_asset, updated_at"
        for name in currency_rate.schema.names:
            if name != "base_asset" and name != "quote_asset" and name != "updated_at":
                sql = sql + ",nvl(1/" + name + ", 0) " + name
        sql = sql + " from markets where base_asset = '" + quote_asset + "' and quote_asset = '" + base_asset + "'"
        currency_rate = spark.sql(sql)

    return currency_rate


def get_asset_data_from_db(fromDate, toDate):
    query = {"updated_at": {"$gte": fromDate, "$lt": toDate}}
    print(query)
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_assets = db["asset_data"]

        return collection_assets.find(query).sort("updated_at", -1).sort("asset_id", 1)


def get_my_assets_from_db():
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_assets = db["assets"]
        return collection_assets.find()


def get_markets_from_db():
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_markets = db["markets"]
        markets = collection_markets.find()
        pdf = pd.DataFrame(list(markets))
        if not pdf.empty:
            del(pdf["_id"])
        return pdf


def get_market_data_from_db(asset_id):
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_market = db["market_data"]

        query = {"base_asset": asset_id, "quote_asset": "USDT"}
        result = collection_market.find(query).sort("updated_at", -1).limit(1)
        for r in result:
            return r
    return {}


def get_last_x_hour_asset_data(x):
    # using now() to get current time
    current_time = datetime.now()
    updated_at_1h_ago = current_time - timedelta(hours=x)
    updated_at_1h_ago = str(updated_at_1h_ago).replace(" ", "T")
    # Printing value of now.
    print("Time now at greenwich meridian is : ")
    current_time = str(current_time).replace(" ", "T")
    print(current_time)
    print(updated_at_1h_ago)
    pdf = pd.DataFrame(list(get_asset_data_from_db(updated_at_1h_ago, current_time)))
    return pdf


def find_drop_ten_percent(df):
    df.createOrReplaceTempView('last_Xh')

    # asset_id name price updated_at
    sql = "SELECT asset_id, group_id, MIN(updated_at) first, MAX(updated_at) last " \
          "FROM last_Xh GROUP BY asset_id, group_id"
    updated_at = spark.sql(sql)
    updated_at.createOrReplaceTempView('updated_at')

    sql = "SELECT u.asset_id, u.group_id, u.first, u.last, l1.price first_price, l2.price last_price, " \
          "l1.price - l2.price diff,  (l1.price - l2.price) * 100 / l1.price percent " \
          "FROM last_Xh l1, last_Xh l2, updated_at u " \
          "WHERE l1.asset_id=u.asset_id AND l2.asset_id=u.asset_id " \
          "AND l1.group_id=u.group_id AND l2.group_id=u.group_id " \
          "AND l1.updated_at=u.first AND l2.updated_at=u.last "
    result = spark.sql(sql)
    result = result.drop("group_id").drop("last").drop("diff").filter(col("percent") < -10).sort(col("percent").asc())
    return result


def find_highest_drop_in_last_24h(df):
    df.createOrReplaceTempView('last_Xh')

    # asset_id name price updated_at
    sql = "SELECT asset_id, group_id, MIN(updated_at) first, MAX(updated_at) last " \
          "FROM last_Xh GROUP BY asset_id, group_id"
    updated_at = spark.sql(sql)
    updated_at.createOrReplaceTempView('updated_at')

    sql = "SELECT u.asset_id, u.group_id, u.first, u.last, l1.price first_price, l2.price last_price, " \
          "l1.price - l2.price diff,  (l1.price - l2.price) * 100 / l1.price percent " \
          "FROM last_Xh l1, last_Xh l2, updated_at u " \
          "WHERE l1.asset_id=u.asset_id AND l2.asset_id=u.asset_id " \
          "AND l1.group_id=u.group_id AND l2.group_id=u.group_id " \
          "AND l1.updated_at=u.first AND l2.updated_at=u.last "
    result = spark.sql(sql)
    result = result.drop("group_id").drop("last").drop("diff").sort(col("percent").asc()).first()
    return result


def find_highest_increase_in_last_24h(df):
    df.createOrReplaceTempView('last_Xh')

    # asset_id name price updated_at
    sql = "SELECT asset_id, group_id, MIN(updated_at) first, MAX(updated_at) last " \
          "FROM last_Xh GROUP BY asset_id, group_id"
    updated_at = spark.sql(sql)
    updated_at.createOrReplaceTempView('updated_at')

    sql = "SELECT u.asset_id, u.group_id, u.first, u.last, l1.price first_price, l2.price last_price, " \
          "l1.price - l2.price diff,  (l1.price - l2.price) * 100 / l1.price percent " \
          "FROM last_Xh l1, last_Xh l2, updated_at u " \
          "WHERE l1.asset_id=u.asset_id AND l2.asset_id=u.asset_id " \
          "AND l1.group_id=u.group_id AND l2.group_id=u.group_id " \
          "AND l1.updated_at=u.first AND l2.updated_at=u.last "
    result = spark.sql(sql)
    result = result.drop("group_id").drop("last").drop("diff").sort(col("percent").desc()).first()
    return result


def find_wave_in_last_24h(df):
    df.createOrReplaceTempView('last_Xh')

    # asset_id name price updated_at
    sql = "SELECT asset_id, group_id, MIN(updated_at) first, MAX(updated_at) last " \
          "FROM last_Xh GROUP BY asset_id, group_id"
    updated_at = spark.sql(sql)
    updated_at.createOrReplaceTempView('updated_at')

    sql = "SELECT u.asset_id, u.group_id, u.first, u.last, l1.price first_price, l2.price last_price, " \
          "l1.price - l2.price diff,  ABS((l1.price - l2.price) * 100 / l1.price) percent " \
          "FROM last_Xh l1, last_Xh l2, updated_at u " \
          "WHERE l1.asset_id=u.asset_id AND l2.asset_id=u.asset_id " \
          "AND l1.group_id=u.group_id AND l2.group_id=u.group_id " \
          "AND l1.updated_at=u.first AND l2.updated_at=u.last "
    result = spark.sql(sql)
    result = result.drop("group_id").drop("last").drop("diff").sort(col("percent").desc()).first()
    return result


def get_dataframe_from_pdf(pdf):
    if pdf.empty:
        return spark.createDataFrame([], StructType([]))
    else:
        # Delete the _id
        if "_id" in pdf:
            del pdf['_id']
        return spark.createDataFrame(pdf)


def drop_ten_percent_in_last_1h():
    pdf = get_last_x_hour_asset_data(4.)
    last_1h = get_dataframe_from_pdf(pdf).drop("name")
    return find_drop_ten_percent(last_1h)


def drop_ten_percent_in_last_24h():
    pdf = get_last_x_hour_asset_data(28.)
    last_1h = get_dataframe_from_pdf(pdf).drop("name")
    return find_drop_ten_percent(last_1h)


def highest_drop_in_last_24h():
    pdf = get_last_x_hour_asset_data(28.)
    last_1h = get_dataframe_from_pdf(pdf).drop("name")
    return find_highest_drop_in_last_24h(last_1h)


def highest_increase_in_last_24h():
    pdf = get_last_x_hour_asset_data(28.)
    last_1h = get_dataframe_from_pdf(pdf).drop("name")
    return find_highest_increase_in_last_24h(last_1h)


def wave_in_last_24h():
    pdf = get_last_x_hour_asset_data(28.)
    last_1h = get_dataframe_from_pdf(pdf).drop("name")
    return find_wave_in_last_24h(last_1h)


def find_highest_price(markets, prices):
    highest_price = 0
    highest_market = ""
    for market in markets.values:
        market = market[0]
        try:
            if prices[market + "_price"] > highest_price:
                highest_price = prices[market + "_price"]
                highest_market = market
        except KeyError as e:
            print(e)
    return {"market": highest_market, "price": highest_price}


def get_my_assets():
    assets = get_my_assets_from_db()

    markets = get_markets_from_db()

    labels = []
    prices = []
    amounts = []

    hightest_asset_price = 0
    hightest_asset = ""
    hightest_asset_index = 0
    index = 0
    for asset in assets:
        price = get_market_data_from_db(asset["currency"])
        price = find_highest_price(markets, price)
        label = asset["currency"] + "/" + price["market"]
        labels.append(label)
        price = round(price["price"] * asset["amount"], 2)
        prices.append(price)
        amounts.append(asset["amount"])

        if price > hightest_asset_price:
            hightest_asset_price = price
            hightest_asset = label
            hightest_asset_index = index

        index = index + 1

    return {"labels": labels, "prices": prices, "amounts": amounts,
            "hightest_asset_index": hightest_asset_index, "hightest_asset": hightest_asset}


def get_alarms_from_db():
    alarms = {}
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_alarms = db["alarms"]
        results = collection_alarms.find({"completed": False})
        for result in results:
            if result["asset_id"] not in alarms:
                alarms[result["asset_id"]] = []
            alarm = {"market": result["market"], "target_price": result["target_price"]}
            alarms[result["asset_id"]] .append(alarm)
    return alarms


def alarm_fired(alarm):
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_alarms = db["alarms"]
        my_query = {"asset_id": alarm["asset_id"], "market": alarm["market"],
                    "target_price": alarm["target_price"], "completed": False}
        new_values = {"$set": {"completed": True}}

        collection_alarms.update_one(my_query, new_values)
