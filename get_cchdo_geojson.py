import requests
import json
from bs4 import BeautifulSoup
import re

from simplify import simplify


def convert_simplified_lon_lat_list(simplified_lon_lat_list):

    coordinates = []

    for lon_lat in simplified_lon_lat_list:

        lon = lon_lat['x']
        lat = lon_lat['y']

        coordinate = [lon, lat]

        coordinates.append(coordinate)

    return coordinates


def collect_lon_lat_to_list(coordinates):

    # Turn coords into list of dicts

    lon_lat_list = []

    for coordinate in coordinates:

        lon_lat_dict = {}

        # format for simplify routine
        lon_lat_dict['x'] = coordinate[0]
        lon_lat_dict['y'] = coordinate[1]

        lon_lat_list.append(lon_lat_dict)

    return lon_lat_list


def simplify_coordinates(coordinates):

    tolerance = 0.0001
    number_of_coordinates = len(coordinates)
    
    starting_lon_lat_list = collect_lon_lat_to_list(coordinates)

    while number_of_coordinates > 500:

        # Collect lat/lon into an list of dicts to apply simplify
        # routine to

        simplified_lon_lat_list = simplify(starting_lon_lat_list, tolerance=tolerance, highestQuality=True)

        number_of_coordinates = len(simplified_lon_lat_list)

        if number_of_coordinates < 100:

            # Revert back one to larger number of coordinates
            # Use starting_lon_lat_list

            break           

        else:             
            tolerance = tolerance + 0.00005
            starting_lon_lat_list = simplified_lon_lat_list

    
    # Convert list of dicts to list of lists for lon/lat
    coordinates = convert_simplified_lon_lat_list(starting_lon_lat_list)

    print('ending # of coordinates', len(coordinates))

    return coordinates



def convert_cchdo_json(bottle_geo, expocode, temporal_coverage):

    if temporal_coverage:

        dates = temporal_coverage.split('/')
        start_date = dates[0]
        end_date = dates[1]
    else:
        start_date = ''
        end_date = ''

    coordinates = []

    for point in bottle_geo:
        lat = point['latitude']
        lon = point['longitude']

        coordinates.append([lon, lat])

    number_of_coordinates = len(coordinates)

    print(number_of_coordinates)

    # Simplify number of coordinates to 100 or less
    if number_of_coordinates > 100:
        coordinates = simplify_coordinates(coordinates)
    
    features = []

    for coord in coordinates:
        feature = {}
        feature["type"] = "Feature"
        feature["geometry"] = {}
        feature["geometry"]["type"] = "Point"
        feature["geometry"]["coordinates"] = coord
        feature["properties"] = {}
        feature["properties"]["expocode"] = expocode
        feature["properties"]["start_date"] = start_date
        feature["properties"]["end_date"] = end_date

        features.append(feature)

    return features


def get_cruise_links():

    all_cruise_links = []
 
    sitemap_url = 'https://cchdo.ucsd.edu/sitemap.xml'

    response = requests.get(sitemap_url)
    html = response.text

    xml_soup = BeautifulSoup(response.text, 'html.parser')

    #<loc>http://cchdo.ucsd.edu/cruise/
    for loc in xml_soup.findAll('loc'):
        link = loc.text

        # Add link if contains cruise in link
        if re.search('/cruise/', link):
            all_cruise_links.append(link)

    return all_cruise_links


def main():

    # delete processing log if it exists
    processing_log = "./geojson_processing_log_tmp.txt"

    geojson_files_log = "./geojson_files_log_tmp.txt"

    # Get list of cchdo cruise links
    cruise_links = get_cruise_links()

    all_features = []

    #cruise_links = ['https://cchdo.ucsd.edu/cruise/320620170820']

    for link in cruise_links:

        #link = 'https://cchdo.ucsd.edu/cruise/32H120030721'

        response = requests.get(link)
        html = response.text

        html_soup = BeautifulSoup(response.text, 'html.parser')

        json_ld = html_soup.select("[type='application/ld+json']")

        ld_json_text = json_ld[0].text

        #print(ld_json_text)

        cchdo_json = json.loads(ld_json_text)

        set_id = cchdo_json['@id']

        expocode = set_id.split('/')[-1]

        try:

            datasets = cchdo_json['dataset']

        except:
            continue

        # find bottle dataset

        for dataset in datasets:

            dataset_id = dataset['@id']
            
            bottle_geo = None
            temporal_coverage = None

            # try to get bottle dataset
            if re.search('#bottle', dataset_id):
                try:
                    bottle_geo = dataset['spatialCoverage']['geo']

                except:
                    pass

                try:
                    temporal_coverage = dataset['temporalCoverage']

                    print(temporal_coverage)

                except:
                    pass

            if bottle_geo:

                features = convert_cchdo_json(bottle_geo, expocode, temporal_coverage)

                all_features.extend(features)

    cchdo_json = {}
    cchdo_json["type"] = "FeatureCollection"
    cchdo_json["features"] = all_features

    with open('cchdo_cruises.json', 'w', encoding='utf-8') as f:
        json.dump(cchdo_json, f)




if __name__ == '__main__':
    main()

