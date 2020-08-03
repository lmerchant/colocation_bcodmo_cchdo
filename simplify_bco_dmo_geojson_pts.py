from pathlib import Path
import json
from simplify import simplify


# Read in BCO-DMO geojson file in output_all folder
# Remove duplicate features
# Count number of features
# More than 500 features?
# then simplify
# else check if more files to process
# If simplify, 
# Collect lat/lon into an array
# Apply simplify routine with set tolerance.
# Expand back to geojson feature for each point
# Save file to output_simplified folder

# Simplify code from
# 
# Alternate code to consder could be from 
# https://github.com/fitnr/visvalingamwyatt


def create_simplified_geojson(simplified_lon_lat_list, geojson):

    features = geojson['features']

    new_features = []

    # geojson meta data same for all points
    metadata = features[0]
    metadata['geometry'] = {}

    # then replace coords
    for lon_lat in simplified_lon_lat_list:

        #print(lon_lat)

        lon = lon_lat['x']
        lat = lon_lat['y']

        geometry = {}
        geometry['coordinates'] = [lon, lat]
        geometry['type'] = 'Point'

        new_feature = metadata.copy()
        new_feature['geometry'] = geometry

        new_features.append(new_feature)

    geojson['features'] = new_features

    return geojson


def collect_lon_lat_to_list(geojson):

    # Turn coords into list of dicts so can apply
    # simplify routine to it
    features = geojson['features']

    lon_lat_list = []

    for feature in features:

        coords = feature['geometry']['coordinates']

        lon_lat_dict = {}

        # format for simplify routine
        lon_lat_dict['x'] = coords[0]
        lon_lat_dict['y'] = coords[1]

        lon_lat_list.append(lon_lat_dict)

    return lon_lat_list


def count_number_of_features(geojson):

    features = geojson['features']

    number_of_features = len(features)

    return number_of_features


def remove_duplicate_features(geojson):

    features = geojson['features']
    new_features = []

    features_set = set()

    for feature in features:

        str_feature = json.dumps(feature)

        if str_feature in features_set:
            continue
        else: 
            features_set.add(str_feature)
            new_features.append(feature)

    geojson['features'] = new_features

    return geojson


def process_data_file(data_file):

    print(data_file)

    # Read geojson from file
    with open(data_file) as json_file:
        geojson = json.load(json_file)

    # Remove duplicate geojson features
    starting_geojson = remove_duplicate_features(geojson)

    simplified_geojson = starting_geojson

    # Count number of features
    number_of_features = count_number_of_features(starting_geojson)
    tolerance = 0.0001

    print('starting # of features', number_of_features)

    # If number of features greater than 100, apply
    # simplify routine
    while number_of_features > 500:

        # Collect lat/lon into an list of dicts
        starting_lon_lat_list = collect_lon_lat_to_list(starting_geojson)

        simplified_lon_lat_list = simplify(starting_lon_lat_list, tolerance=tolerance, highestQuality=True)

        # Expand back to geojson feature for each point
        simplified_geojson = create_simplified_geojson(simplified_lon_lat_list, starting_geojson)

        number_of_features = count_number_of_features(simplified_geojson)

        if number_of_features < 100:

            # Revert back one to larger number of features
            simplified_geojson = starting_geojson

            number_of_features = count_number_of_features(simplified_geojson)

            break           

        else: 
            starting_geojson = simplified_geojson
            tolerance = tolerance + 0.0005

    print('ending # of features', number_of_features)

    # Save geojson to file
    output_folder = './output_simplified_jsonld_geojson_ctd_check'


    # get filename of data_file
    filename = Path(data_file).name

    output_file = Path(output_folder, filename)
    with open(output_file, 'w') as f:
        json.dump(simplified_geojson, f)


def main():

    #data_folder = Path('./output_all')
    data_folder = Path('./output_erddap_modified_geojson_jsonld_dates_ctd_check')


    for file in data_folder.iterdir():
        if file.suffix == '.json':

            #file = './output_all/3296.json'

            process_data_file(file)

        #break


if __name__ == '__main__':
    main()
