import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure


def usd_value_total_24h_chart(data):
    print(data)
    sent_times_ts = pd.to_datetime(data['updated_at'])

    fig = Figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.subplots()
    ax.margins(0.2, 0.2)

    ax.plot(sent_times_ts, 'price', data=data)

    # Major ticks every 10 minutes.
    major_locator = mdates.HourLocator(interval=4)
    ax.xaxis.set_major_locator(major_locator)

    # Minor ticks every 30 minutes.
    minor_locator = mdates.HourLocator(interval=1)
    ax.xaxis.set_minor_locator(minor_locator)

    # Text in the x axis will be displayed in 'YYYY-mm' format.
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Format the coords message box, i.e. the numbers displayed as the cursor moves
    # across the axes within the interactive GUI.
    ax.format_xdata = mdates.DateFormatter('%H:%M')
    ax.format_ydata = lambda x: f'${x:.2f}'  # Format the price.
    ax.grid(True)

    fig.autofmt_xdate()

    plt.xlabel("Time")
    plt.ylabel("Price")

    return fig


def usd_value_pie_chart(my_assets):
    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    explodes = [0] * len(my_assets["prices"])
    explodes[my_assets["hightest_asset_index"]] = 0.1

    fig = Figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.subplots()
    ax.margins(0.2, 0.2)

    ax.pie(my_assets["prices"], explode=tuple(explodes), labels=tuple(my_assets["labels"]),
           autopct='%1.1f%%', shadow=True, startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    return fig


def last_24h_price_time_graph(asset_id, data):
    # print(data)
    sent_times_ts = pd.to_datetime(data['updated_at'])

    fig = Figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.subplots()
    ax.margins(0.2, 0.2)

    ax.plot(sent_times_ts, 'price', data=data)

    # Major ticks every 10 minutes.
    major_locator = mdates.HourLocator(interval=4)
    ax.xaxis.set_major_locator(major_locator)

    # Minor ticks every 30 minutes.
    minor_locator = mdates.HourLocator(interval=1)
    ax.xaxis.set_minor_locator(minor_locator)

    # Text in the x axis will be displayed in 'YYYY-mm' format.
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Format the coords message box, i.e. the numbers displayed as the cursor moves
    # across the axes within the interactive GUI.
    ax.format_xdata = mdates.DateFormatter('%H:%M')
    ax.format_ydata = lambda x: f'${x:.2f}'  # Format the price.
    ax.grid(True)

    fig.autofmt_xdate()

    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.title(str(asset_id) + " Price")

    return fig
