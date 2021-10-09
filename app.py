import io
import json
from pymongo import MongoClient
from kafka import KafkaConsumer, TopicPartition
import base64
import uuid
from io import BytesIO
from pyspark.sql.functions import col, UserDefinedFunction
from pyspark.sql.types import *

# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
from flask import Flask, send_from_directory, send_file
from flask_cors import cross_origin, CORS
from flask_socketio import SocketIO, emit
from flask import request
# Flask constructor takes the name of
# current module (__name__) as argument.

# My Libraries
import my_library
import graphs

# Initialize
#assets = {}
#for asset in my_library.get_assets():
#    assets[asset["asset_id"]] = []
MONGODB_HOST = 'mongodb://localhost:27017/?readPreference=primary&appname=BigDataProject&ssl=false'
MONGODB_DBNAME = 'BIG_DATA_PROJECT'

# MongoDB init
connection = MongoClient(MONGODB_HOST)

# Flask init
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/')
@cross_origin()
def home():
    return send_from_directory('.', "index.html")


#
# Charts
#
@app.route('/usd_value_pie_chart')
def usd_value_pie_chart():
    # Generate the figure **without using pyplot**.
    my_assets = my_library.get_my_assets()
    fig = graphs.usd_value_pie_chart(my_assets)

    # Save it to a temporary buffer.
    buf = BytesIO()
    fig.savefig(buf, format="png")
    # Embed the result in the html output.
    # data = base64.b64encode(buf.getbuffer()).decode("ascii")
    # return f"<img src='data:image/png;base64,{data}'/>"
    return send_file(io.BytesIO(buf.getbuffer()),
                     download_name='usd_value_pie_chart.png',
                     mimetype='image/png')


def get_hour(updated_at):
    return updated_at[11:16]


@app.route('/last_24h_price_time_graph')
def last_24h_price_time_graph():
    asset_id = request.args.get('asset_id')
    print("A:", asset_id)
    # Generate the figure **without using pyplot**.
    asset_list = my_library.get_last_x_hour_asset_data(28)
    if "_id" in asset_list:
        del asset_list['_id']
    assets_df = local_df = my_library.spark.createDataFrame(asset_list)
    assets_df = assets_df.filter(col("asset_id") == asset_id).sort(col("updated_at"))

    get_hour_udf = UserDefinedFunction(get_hour, StringType())
    assets_df = assets_df.withColumn("deneme", get_hour_udf('updated_at'))

    assets_df.show(truncate=False)
    data = assets_df.toPandas()

    fig = graphs.last_24h_price_time_graph(asset_id, data)

    # Save it to a temporary buffer.
    buf = BytesIO()
    fig.savefig(buf, format="png")
    return send_file(io.BytesIO(buf.getbuffer()),
                     download_name='usd_value_pie_chart.png',
                     mimetype='image/png')


#
# SocketIO endpoints
#
@socketio.on('get_my_assets', namespace='/kafka')
def get_my_assets(message):
    my_assets = my_library.get_my_assets()
    labels = my_assets["labels"]
    prices = my_assets["prices"]
    amounts = my_assets["amounts"]
    html = "<table border='1'><tr><th>Varlık / En yüksek fiyatlı market</th><th>Miktar</th><th>USD Karşılığı</th></tr>"
    for i in range(len(labels)):
        html = html + f"<tr><td>{labels[i]}</td><td align='right'>{amounts[i]}</td><td align='right'>{prices[i]}</td></tr>"
    html = html + "</table>"
    emit('get_my_assets', {'data': html})


@socketio.on('add_asset', namespace='/kafka')
def add_asset(message):
    message = json.loads(message)
    asset_id = message["currency"]
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_assets = db["assets"]

    my_query = {"currency": asset_id}
    if collection_assets.count_documents(my_query) == 0:
        collection_assets.insert_one(message)
    else:
        collections = collection_assets.find(my_query)
        message["amount"] = list(collections)[0]["amount"] + message["amount"]

        new_amount = {"$set": {"amount": message["amount"]}}
        collection_assets.update_one(my_query, new_amount)
    emit('update_my_assets', {'data': 'Update requested.'})


@socketio.on('get_currencies', namespace='/kafka')
def get_currencies(message):
    if MONGODB_DBNAME in connection.list_database_names():
        db = connection[MONGODB_DBNAME]
        collection_currencies = db["asset_data"]
        collections = collection_currencies.distinct("asset_id")
        emit('get_currencies', {'data': json.dumps(list(collections))})


@socketio.on('connect', namespace='/kafka')
def test_connect():
    emit('logs', {'data': 'Connection established'})


@socketio.on('get_report_from_api', namespace='/kafka')
def get_report_from_api(message):
    print("get_report_from_api")
    print(message)
    print("DEACTIVATED!")
    # report = my_library.create_asset_report_from_api()
    # emit('volume_24h_first', {'data': report["first_five_in_volume_24h"]})
    # emit('volume_24h_last', {'data': report["last_five_in_volume_24h"]})
    # emit('change_1h_first', {'data': report["first_five_in_change_1h"]})
    # emit('change_1h_last', {'data': report["last_five_in_change_1h"]})


@socketio.on('get_report_from_db', namespace='/kafka')
def get_report_from_db(message):
    print("get_report_from_db")
    print(message)
    report = my_library.create_asset_report_from_db()
    emit('drop_ten_percent_in_last_1h', {'data': report["drop_ten_percent_in_last_1h"]})
    emit('drop_ten_percent_in_last_24h', {'data': report["drop_ten_percent_in_last_24h"]})
    emit('highest_drop_in_last_24h', {'data': report["highest_drop_in_last_24h"]})
    emit('wave_in_last_24h', {'data': report["wave_in_last_24h"]})
    emit('highest_drop_asset', {'data': report["highest_drop_asset"]})
    emit('highest_increase_asset', {'data': report["highest_increase_asset"]})


@socketio.on('kafka_consumer', namespace="/kafka")
def kafka_consumer(message_from_ui):
    print("Kafka Consumer:", message_from_ui)
    # Kafka init
    ui_alert_topic = "ui_alert"
    consumer = KafkaConsumer(group_id='consumer-ui', bootstrap_servers='localhost:9092',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    tp = TopicPartition(ui_alert_topic, 0)
    # register to the topic
    consumer.assign([tp])

    consumer.seek_to_end(tp)
    for message in consumer:
        value = message.value
        alert = value["alert"]
        alarm_id = uuid.uuid1()
        response = "<div id ='" + str(alarm_id) + "'>" + str(alert["asset_id"]) + "'nin değeri " + \
                   str(alert["market"]) + "'de " + str(alert["price"]) + " oldu.</div>"
        emit('kafka_consumer', {'data': response})
        emit('alert_remove', {'data': str(alarm_id)})
        print("message.offset ", message.offset)


# main driver function
if __name__ == '__main__':
    # run() method of SocketIO/Flask class runs the application
    # on the local development server.
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
