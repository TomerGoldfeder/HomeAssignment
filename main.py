import requests
import pandas as pd


'''
Simple data fetch using requests
'''
def fetch_data(url):
    req = requests.get(url)
    req.raise_for_status()
    return req.json()


'''
Based on the question criteria
'''
def get_color(broken_docks):
    if broken_docks <= 10:
        return "green"
    elif 10 < broken_docks < 30:
        return "yellow"
    else:
        return "red"


'''
The number of brokenDocks are the non functional docks in the station.
According to the json we receive we get the followings:
1. "availableDocks"
2. "totalDocks"
we also get "availableBikes" but we assume that they are included in the availableDocks

In order to get the number of brokenDocks we will subtract the number of availableDocks from the totalDocks.

From the only response we are getting in this example json, i assume that each station in the stationBeanList is unique
'''
def enrich_data(data, station_color_truth):
    station_name = '{}Stations'.format(station_color_truth.capitalize())
    exec_date = data['executionTime'].split()[0]  # i.e: 2020-12-16 10:04:20 AM  --> taking only the first part
    data_list = data['stationBeanList']
    count = 0
    # iterate over the list and assign each station a color
    for d in data_list:
        d['brokenDocks'] = d['totalDocks'] - d['availableDocks']
        d['stationColor'] = get_color(d['brokenDocks'])
        if d['stationColor'] == station_color_truth: count += 1
    return {"date": exec_date, "data": data_list, station_name: count}


'''
The data we need to create the graph requested we need the followings:
1. time dates as the xticks
2. the max number of red-stations as the max height for the graph (we will add 5% so the graph will look normal)
3. the data points as follow:
    3.1. list of integers from 1 to length of dates (xticks)
    3.2. the value of red-station per date

The assumption here is that this script will run eventually on a cloud service like AWS.
Another assumption is that the data will to go through a Messaging Broker like Kinesis or Kafka as a stream and trigger
a Lambda function which will include this script and save the data into a RDS using boto package and we can make queries on that db

In this example we will assume that there is a location where we can write data as a csv file and update it.
We will also assume that this script runs indefinitely as it will accumulate the data, only for the example we will
use a for loop which can be set manually 

The data for the graph will be saved as follow: 
Date       | RedStations
-------------------------
2020-12-16 | 10
'''
def save_data(processed_data, save_location, station_color_truth):
    # try to get the previous written csv, if not then this is the first time in this location
    try:
        df = pd.read_csv(save_location)
    except:
        df = None

    # adjust the station name according to the color
    station_name = '{}Stations'.format(station_color_truth.capitalize())
    # create the data frame with the new data
    new_df = pd.DataFrame({
        "Date": [processed_data['date']],
        station_name: [processed_data[station_name]]
    })
    # append if needed
    if df is not None:
        new_df = pd.concat([df, new_df], axis=0)

    # save into a csv file
    new_df = new_df[['Date', station_name]]
    new_df.to_csv(save_location, index=False)


def start_data_ingestion(url, station_color_truth, save_location, runs=3):
    for i in range(runs):
        data = fetch_data(url)
        processed_data = enrich_data(data, station_color_truth)
        save_data(processed_data, save_location, station_color_truth)


if __name__ == "__main__":
    citibikenyc_url = "http://citibikenyc.com/stations/json"

    # We will make a generic script so it will be able to get all colors stations availability
    base_path = "."
    station_color_truth = "red"
    accesible_location = "{}/{}_stations_statistics.csv".format(base_path, station_color_truth)
    iterations = 1
    start_data_ingestion(url=citibikenyc_url, station_color_truth=station_color_truth, save_location=accesible_location, runs=iterations)
    _df = pd.read_csv(accesible_location)
    print(_df)