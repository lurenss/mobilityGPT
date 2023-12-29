import datetime
import geopandas as gpd
import numpy as np
import requests
import json
import polyline
import datetime
import pandas as pd 
from tqdm import tqdm
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--people', type=int, default=1000, help='number of people to generate')
    parser.add_argument('--start', type=str, default='2023-02-06', help='start date of the simulation e.g 2023-02-06')
    parser.add_argument('--end', type=str, default='2023-02-10' , help='end date of the simulation e.g 2023-02-10')
    parser.add_argument('--file', type=str, default='gps_traces.csv', help='name of the file to save the gps traces')
    parser.add_argument('--ext', type=str, default='csv', help='extension of the file to save the gps traces')
    args = parser.parse_args()

    # check if the extension is csv or parquet
    if args.ext != 'csv' and args.ext != 'parquet':
        print('The extension must be csv or parquet')
        exit()

    number_of_people = args.people
    group_of_people = ['high_school_student', 'college_student', 'adult', 'elderly']

    motifs = {
            'high_school_student': ['home', 'school', 'home','sport_centre','home'],
            'college_student': ['home', 'university', 'cafe', 'library', 'pub','home'],
            'adult': ['home', 'office', 'gym', 'supermarket','home'],
            'elderly': ['home', 'bar', 'supermarket', 'pharmacy','home']
        }

    people_distribution = {
            'high_school_student': 0.2,
            'college_student': 0.2,
            'adult': 0.4,
            'elderly': 0.2
        }

    # generate people and store them in a dictionary using an id as key and assing a group of people to each person according to the distribution
    people = {}
    for i in range(0, number_of_people):
        people[i] = {
            'group': np.random.choice(group_of_people, p=list(people_distribution.values()))
        }

    #############################################################
    # assign home location to each persone using the residential area given by the shapefile
    landuse_gdf = gpd.read_file('./shp_padova/landuse.shp')
    landuse_gdf = landuse_gdf.to_crs(epsg=4326)

    # finf the areas that have column type = residential
    residential_areas = landuse_gdf[landuse_gdf['type'] == 'residential']

    # assing home location to each person

    for i in range(0, number_of_people):
        poligon = residential_areas.sample(1)['geometry'].values[0]
        point = poligon.representative_point()
        people[i]['home_location'] = point
        print(people[i]['home_location'])


    # collect all the pois from buildings shapefile
    buildings_gdf = gpd.read_file('./shp_padova/buildings.shp')
    buildings_gdf = buildings_gdf.to_crs(epsg=4326)
    pois = buildings_gdf

    # make a dictionary of pois using as key the type of poi and as value the list of coordinates of that type
    pois_dict = {}
    for index, poi in pois.iterrows():
        if poi.type not in pois_dict:
            pois_dict[poi.type] = []
        pois_dict[poi.type].append(poi.geometry)


    def get_nearest_poi(point, pois):
        # get the nearest poi
        nearest_poi = pois[0]
        for poi in pois:
            if point.distance(poi) < point.distance(nearest_poi):
                nearest_poi = poi
        return nearest_poi


    def generate_gps_trace_from_to(start_point, end_point, unix_start_time):
        # extract longitude and latitude from the points
        start_point = (start_point.x, start_point.y)
        end_point = (end_point.x, end_point.y)
        # Convert the points to strings
        start = ','.join(map(str, start_point))
        end = ','.join(map(str, end_point))

        response = requests.get(f'http://router.project-osrm.org/route/v1/driving/{start};{end}?overview=full')
        data = json.loads(response.text)

        polyline_data = polyline.decode(data['routes'][0]['geometry'])



        # Extract the travel time in seconds from the response
        travel_time_in_seconds = data['routes'][0]['duration']
        print('travel_time_in_seconds:', travel_time_in_seconds)
        print('len(polyline_data):', len(polyline_data))


        # Calculate the time increment for each point
        time_increment = int(travel_time_in_seconds / len(polyline_data))
        print('time_increment:', time_increment)


        # Add a timestamp to each coordinate
        coordinates_with_timestamps = []
        for i, coordinate in enumerate(polyline_data):
            # Calculate the timestamp for the coordinate
            timestamp = unix_start_time + i * time_increment
            coordinates_with_timestamps.append((coordinate[1], coordinate[0], timestamp)) 

        return coordinates_with_timestamps

    # generate a workinf week of gps traces for each person for february 2023
    start_date = datetime.datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(args.end, '%Y-%m-%d')
    delta = datetime.timedelta(days=1)

    lst_days = []
    while start_date <= end_date:
        lst_days.append(start_date)
        start_date += delta

    # Create an empty dataframe to store the origin and destination data
    df = pd.DataFrame(columns=['Date','User', 'origin_timestamp', 'destination_timestamp', 'origin_coordinates', 'destination_coordinates','path'])

    # generate Origin and Destination trajectoris for each person and for each day
    for day in tqdm(lst_days):
        unix_start_time_temp = int(start_date.replace(hour=7).timestamp())
        for i in range(0, number_of_people):
            unix_start_time = np.random.randint(unix_start_time_temp - 3600, unix_start_time_temp + 3600)
            # get the motif for the group of people of the person
            motif = motifs[people[i]['group']]
            # get the home location of the person
            home_location = people[i]['home_location']
            # iterate over all couples of movement in the motif
            for j in range(len(motif) - 1):
                if motif[j] == 'home':
                    origin = home_location
                else:
                    origin = get_nearest_poi(home_location, pois_dict[motif[j]])
                    origin = origin.representative_point()
                
                if motif[j + 1] == 'home':
                    destination = home_location
                else:
                    destination = get_nearest_poi(home_location, pois_dict[motif[j + 1]])
                    destination = destination.representative_point()
                
                # generate the gps trace
                gps_trace = generate_gps_trace_from_to(origin, destination, unix_start_time)

                # add the gps trace to the dataframe
                df = df._append({
                    'Date': day,
                    'User': i,
                    'origin_timestamp': gps_trace[0][2],
                    'destination_timestamp': gps_trace[-1][2],
                    'origin_coordinates': origin,
                    'destination_coordinates': destination,
                    'path': gps_trace
                }, ignore_index=True)

                # update the unix_start_time
                if motif[j + 1] == 'home':
                    unix_start_time += 3600 * np.random.randint(2, 4)
                elif motif[j + 1] == 'school':
                    unix_start_time += 3600 * np.random.randint(5, 7)
                elif motif[j + 1] == 'university':
                    unix_start_time += 3600 * np.random.randint(2, 6)
                elif motif[j + 1] == 'cafe':
                    unix_start_time += 3600 * np.random.randint(1, 2)
                elif motif[j + 1] == 'library':
                    unix_start_time += 3600 * np.random.randint(2, 4)
                elif motif[j + 1] == 'pub':
                    unix_start_time += 3600 * np.random.randint(1, 3)
                elif motif[j + 1] == 'office':
                    unix_start_time += 3600 * np.random.randint(6, 10)
                elif motif[j + 1] == 'gym':
                    unix_start_time += 3600 * np.random.randint(1, 2)
                elif motif[j + 1] == 'supermarket':
                    unix_start_time += 3600 * 1
                elif motif[j + 1] == 'bar':
                    unix_start_time += 3600 * np.random.randint(1, 2)
                elif motif[j + 1] == 'pharmacy':
                    unix_start_time += 3600 * 0.5
                elif motif[j + 1] == 'sport_centre':
                    unix_start_time += 3600 * np.random.randint(1, 2)
    # conver origin column to string
    df['origin_coordinates'] = df['origin_coordinates'].astype(str)
    df['destination_coordinates'] = df['destination_coordinates'].astype(str)

    if args.ext == 'csv':
        df.to_csv(args.file, index=False)
    elif args.ext == 'parquet':
        df.to_parquet(args.file, index=False)
    