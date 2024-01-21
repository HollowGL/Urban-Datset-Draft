import pickle

import numpy as np
import pandas as pd
from tqdm import tqdm


def glance():
    with open(r'Public_Datasets\Taxi\15_minutes\Taxi_Chicago\Taxi_Chicago.pkl', 'rb') as f:
        model = pickle.load(f)
        print()


def process():
    CSV_PATH = r'C:\Users\guil\Downloads\MTA_Bus_Hourly_Ridership__Beginning_February_2022_20240121.csv'
    df = pd.read_csv(CSV_PATH)
    """ (1 + 27740895) rows x 6 columns
     - transit_timestamp: from 2022/2/1 to 2024/1/20, 5:00AM to 11:00PM, 1 hour interval
     - bus_route: B1 to B100, and more ???
     - payment_method
     - fare_class_category
     - ridership
     - transfers
    """
    # df = df.iloc[:1000]

    time_slots = df['transit_timestamp'].unique()
    time_slot_map = {time: idx for idx, time in enumerate(time_slots)}
    bus_routes = df['bus_route'].unique()
    bus_route_map = {route: idx for idx, route in enumerate(bus_routes)}

    data = np.zeros(shape=(len(time_slots), len(bus_routes)), dtype=np.int32)
    
    for _, row in tqdm(df.iterrows()):
        flow = row['ridership'] + row['transfers']
        time = row['transit_timestamp']
        time_idx = time_slot_map[time] 
        route = row['bus_route']
        route_idx = bus_route_map[route]
        data[time_idx][route_idx] = flow


    result_df = pd.DataFrame(data, columns=bus_routes)
    result_df.insert(0, 'time', time_slots)
    
    # Save data as CSV
    result_df.to_csv('data.csv', index=False)
        

    DataFormat = {
        "TimeRange": ['2022-2-1', '2024-1-20'],    # 起止时间 str eg:['2016-10-01', '2016-11-30']
        "TimeFitness": 60,  # 时间粒度 int 单位为min
        "Node": {
            "TrafficNode": data,  # np.array, with shape [time_slots, num-of-node] eg:(1440,256) 
            "TrafficMonthlyInteraction": [], # np.array, With shape [month, num-of-node. num-of-node]
            "StationInfo": [],  # list of [id, build-time, lat, lng, name], eg:['0', 0, 34.210542575000005, 108.91390095, 'grid_0']
            "POI": []
        },
        "Grid": {
            "TrafficGrid": [], # with shape [slots, num-of-node. num-of-node] eg:(120, 256, 256)
            "GridLatLng": [],  # 对角线点的经纬度 eg:[[34.20829427, 108.91118]]
            "POI": []
        },
        "ExternalFeature": {
            "Weather": []
        }
    }

    with open('data.pkl', 'wb') as f:
        pickle.dump(DataFormat, f)


if __name__ == '__main__':
    process()