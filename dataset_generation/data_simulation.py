import datetime
import geopandas as gpd
import numpy as np
import requests
import json
import polyline
import datetime
import pandas as pd 
from tqdm import tqdm

number_of_people = 1000
group_of_people = ['high_school_student', 'college_student', 'adult', 'elderly']

motifs = {
        'high_school_student': ['home', 'school', 'sport_centre','home'],
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
buildings_gdf = gpd.read_file('shp_padova/buildings.shp')
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
start_date = datetime.datetime.strptime('2023-02-06', '%Y-%m-%d')
end_date = datetime.datetime.strptime('2023-02-10', '%Y-%m-%d')
delta = datetime.timedelta(days=1)

# Create an empty dataframe to store the origin and destination data
df = pd.DataFrame(columns=['Date','User', 'origin_timestamp', 'destination_timestamp', 'origin_coordinates', 'destination_coordinates','path'])

while start_date <= end_date:
    print(start_date.strftime('%Y-%m-%d'))
    date = start_date.strftime('%Y-%m-%d')
    unix_start_time = int(start_date.replace(hour=0).timestamp())
    for person_id, person in tqdm(people.items()):

        point_a = person['home_location']
        unix_start_time_temp = int(start_date.replace(hour=7).timestamp())
        for motif in motifs[person['group']]:
            gps_trace = []

            # randomly perturbate the timestamp of the day of 7 am
            unix_start_time = np.random.randint(unix_start_time_temp - 3600, unix_start_time_temp + 3600)
            if motif != 'home':
                # get the pois of that type
                pois_of_that_type = pois_dict[motif]
                # choose the nearest poi of that type
                nearest_poi = get_nearest_poi(point_a, pois_of_that_type).representative_point()
                # generate gps trace from home to the nearest poi
                gps_trace.extend(generate_gps_trace_from_to(point_a, nearest_poi, unix_start_time))
                origin = point_a
                point_a = nearest_poi

                unix_time_arrival = gps_trace[-1][2]

                # add the row to the dataframe
                df = df._append({'Date': date, 'User': person_id,  'origin_timestamp': unix_start_time, 'destination_timestamp': unix_time_arrival, 'origin_coordinates': point_a, 'destination_coordinates': nearest_poi, 'path': gps_trace}, ignore_index=True)

                # for each motif add an appropriate time interval of activity in the poi
                if motif == 'school':
                    unix_start_time += 3600 * np.random.randint(5, 7)
                elif motif == 'university':
                    unix_start_time += 3600 * np.random.randint(2, 6)
                elif motif == 'cafe':
                    unix_start_time += 3600 * np.random.randint(1, 2)
                elif motif == 'library':
                    unix_start_time += 3600 * np.random.randint(2, 4)
                elif motif == 'pub':
                    unix_start_time += 3600 * np.random.randint(1, 3)
                elif motif == 'office':
                    unix_start_time += 3600 * np.random.randint(6, 10)
                elif motif == 'gym':
                    unix_start_time += 3600 * np.random.randint(1, 2)
                elif motif == 'supermarket':
                    unix_start_time += 3600 * 1
                elif motif == 'bar':
                    unix_start_time += 3600 * np.random.randint(1, 2)
                elif motif == 'pharmacy':
                    unix_start_time += 3600 * 0.5
                elif motif == 'sport_centre':
                    unix_start_time += 3600 * np.random.randint(1, 2)
                
            else:
                # append the home location to the gps trace
                gps_trace.append((point_a.x, point_a.y, unix_start_time))

                # add the row to the dataframe
                df = df._append({'Date': date, 'User': person_id, 'origin_timestamp': unix_start_time, 'destination_timestamp': unix_start_time, 'origin_coordinates': point_a, 'destination_coordinates': person['home_location'], 'path': gps_trace}, ignore_index=True)

    start_date += delta
    unix_start_time = int(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp())

df.to_csv('gps_traces.csv', index=False)