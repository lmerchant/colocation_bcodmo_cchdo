import json
import os
from pathlib import Path


def main():


    # Loop through Bco-dmo files in output folder
    # then concatenate them. 

    all_features = []

    #data_folder = Path('./output_simplified')
    data_folder = Path('./output_simplified_jsonld_geojson_ctd_check')

    for filename in data_folder.iterdir():

        if filename.suffix == '.json':

            with open(filename) as f:
                data = json.load(f)
                features = data["features"]

                all_features.extend(features)

    bco_dmo_json = {}
    bco_dmo_json["type"] = "FeatureCollection"
    bco_dmo_json["features"] = all_features

    with open('bco-dmo_cruises.json', 'w', encoding='utf-8') as f:
        json.dump(bco_dmo_json, f)



if __name__ == '__main__':
    main()

