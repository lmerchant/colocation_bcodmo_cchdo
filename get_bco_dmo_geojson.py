import re
from bs4 import BeautifulSoup
import requests
import json
import os
import numpy as np
import pandas as pd
import itertools

import search_pages_meta_data as meta
import check_if_ctd_data as ctd
import simplify


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


def convert_json_to_geojson(bco_dmo_json):

    coordinates = bco_dmo_json['geometry']['track']['coordinates']

    features = []

    for coord in coordinates:
        feature = {}
        feature["type"] = "Feature"
        feature["geometry"] = {}
        feature["geometry"]["type"] = "Point"
        feature["geometry"]["coordinates"] = coord
        feature["properties"] = {}
        feature["properties"]["dataset_id"] = bco_dmo_json["dataset_id"]
        feature["properties"]["title"] = bco_dmo_json["title"]
        feature["properties"]["chief_scientist"] = bco_dmo_json["chief_scientist"]
        feature["properties"]["start_date"] = bco_dmo_json["start_date"]
        feature["properties"]["end_date"] = bco_dmo_json["end_date"]
        feature["properties"]["deployments"] = bco_dmo_json["deployments"]
        feature["properties"]["platforms"] = bco_dmo_json["platforms"]

        features.append(feature)

    bco_dmo_geojson = {}
    bco_dmo_geojson["type"] = "FeatureCollection"
    bco_dmo_geojson["features"] = features

    return bco_dmo_geojson


def simplify_lon_lat_list(lon_lat_list):

    # convert to numeric and apply simplify routine
    lon_lat_list = np.array(lon_lat_list, dtype=np.float32)

    lon_lat_list = np.array(lon_lat_list).tolist()

    tolerance = 0.5
    highQuality = True

    lon_lat_list = simplify.simplify(lon_lat_list, tolerance, highQuality)

    # Convert back to strings
    lon_lat_list = np.array(lon_lat_list)
    lon_lat_list = lon_lat_list.astype(str)
    lon_lat_list = lon_lat_list.tolist()

    return lon_lat_list


def get_lon_lat_list(df, pressure_col_index, latitude_col_index, longitude_col_index):

    # remove rows where cell has nan value
    df = df[df.iloc[:,pressure_col_index].notnull()]
    df = df[df.iloc[:,latitude_col_index].notnull()]
    df = df[df.iloc[:,longitude_col_index].notnull()]

    # remove rows where nd in lat or lon column
    df = df[ df.iloc[:,latitude_col_index] != 'nd']
    df = df[ df.iloc[:,longitude_col_index] != 'nd']

    df = df.iloc[:,[latitude_col_index, longitude_col_index]].copy()

    df = df.apply(pd.to_numeric)
    df = df.dropna()
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    lon_lat_list = []

    for index, row in df.iterrows():

        lon_lat_pair = df.iloc[index, [1,0]].values.tolist()
        lon_lat_list.append(lon_lat_pair)

    return lon_lat_list


def get_platforms(dataset_soup):

    # <a href="/platform/53992">R/V Endeavor</a>
    links = dataset_soup.find_all('a', href = re.compile(r'/platform/\d+'))

    platforms = [link.string for link in links]

    return platforms


def get_deployments(dataset_soup):

    # <a href="/deployment/57739">EN198</a> 
    links = dataset_soup.find_all('a', href = re.compile(r'/deployment/\d+'))

    deployments = [link.string for link in links]

    return deployments


def get_temporal_coverage(dataset_soup):

    # Find "temporalCoverage": "1999-03-29/1999-06-28" if exists in html

    try:
        data = dataset_soup.select("[type='application/ld+json']")[1]

        temporal_coverage = json.loads(data.text)["temporalCoverage"]

        return temporal_coverage

    except:

        return None


def get_start_date(dataset_soup):

    # <td class="views-field views-field-field-deployment-start-date nowrap" >
    #<span class="date-display-single" property="dc:date" datatype="xsd:dateTime" content="1989-06-28T04:00:00-04:00">1989-06-28</span>          
    #</td>
    td = dataset_soup.find('td', {'class': 'views-field-field-deployment-start-date'})
    span = td.find('span')
    start_date = span.string

    return start_date


def get_start_end_dates(dataset_soup):

    start_date = get_start_date(dataset_soup)

    # See if json+ld exists for page and grab temporal coverage
    temporal_coverage = get_temporal_coverage(dataset_soup)

    if temporal_coverage:

        dates = temporal_coverage.split('/')

        start_date = dates[0]
        end_date = dates[1]

    else:
        end_date = None   

    return start_date, end_date


def create_bco_dmo_json(index, dataset_id, titles, investigators, dataset_soup, lon_lat_list):

    bco_dmo_json = {}

    # Create bco-dmo json
    bco_dmo_json["dataset_id"] = dataset_id
    bco_dmo_json["title"] = titles[index]
    bco_dmo_json["chief_scientist"] = investigators[index]

    start_date, end_date = get_start_end_dates(dataset_soup)

    bco_dmo_json["start_date"] = start_date
    bco_dmo_json["end_date"] = end_date

    bco_dmo_json["deployments"] = get_deployments(dataset_soup)
    bco_dmo_json["platforms"] = get_platforms(dataset_soup)        

    bco_dmo_json["geometry"] = {"track":{}}
    bco_dmo_json["geometry"]["track"] = {"coordinates":{}}
    bco_dmo_json['geometry']['track']['coordinates'] = lon_lat_list
    bco_dmo_json['geometry']['track']['type'] = "LineString"

    return bco_dmo_json


def save_geojson(dataset_id, geojson):

    # Save json to file
    filename = str(dataset_id) + '.json'

    filepath = './output_geojson/' + filename

    with open(filepath, 'w') as f:
        json.dump(geojson, f, indent=4, sort_keys=True) 


def get_geojson_url(dataset_id):

    #return f"https://erddap.bco-dmo.org/erddap/tabledap/bcodmo_dataset_{str(dataset_id)}.geoJson"

    return f"https://erddap.bco-dmo.org/erddap/tabledap/bcodmo_dataset_{str(dataset_id)}.geoJson"


def get_geojson(dataset_id, processing_log):

    url = get_geojson_url(dataset_id)

    r = requests.get(url)

    if r.status_code != 200:

        with open(processing_log, 'a+') as f:
            text = f"No CTD geoJSON file at dataset id: {dataset_id}\n"
            f.write(text)   
                     
        return None

    return r.json()


def get_dataset_url(dataset_id):

    url = f'http://www.bco-dmo.org/dataset/{dataset_id}'
    return url    


def get_dataset_soup(dataset_id, processing_log, page, number_of_datasets_per_page):

    url = get_dataset_url(dataset_id)

    try:
        response = requests.get(url)
        dataset_soup = BeautifulSoup(response.text, 'html.parser')

    except requests.exceptions.RequestException as e:
        # response doesn't exist
        dataset_soup = None

        # Can't reach data page
        # Log it
        with open(processing_log, 'a+') as f:
            text = f"Site not reached for dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(text)

    # No dataset available
    if dataset_soup and not dataset_soup(text='Data URL:'):

        with open(processing_log, 'a+') as f:
            text = f"No data set for id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(text)

        dataset_soup = None

    return dataset_soup


def get_bco_dmo_json(index, page, number_of_datasets_per_page, dataset_id, titles, investigators, processing_log):

    # TODO: process zip files

    bco_dmo_json = {}

    dataset_soup = get_dataset_soup(dataset_id, processing_log, page, number_of_datasets_per_page)

    if not dataset_soup:
        # No BCO-DMO json to create
        return {}

    # Put data into a dataframe
    dataset_df = ctd.put_dataset_into_dataframe(dataset_id, processing_log, page, index, number_of_datasets_per_page)
    
    if dataset_df is None:
        # Can't create dataframe
        return {}

    # Check dataframe to see if CTD data
    column_names = dataset_df.columns

    is_ctd, pressure_col_index, latitude_col_index, longitude_col_index = ctd.check_column_names_for_ctd(column_names)

    if is_ctd:

        lon_lat_list = get_lon_lat_list(dataset_df, pressure_col_index, latitude_col_index, longitude_col_index)

        # TODO: Fix so only simplify if file has more 
        # than say 100 points
        #lon_lat_list = simplify_lon_lat_list(lon_lat_list)

        ctd_files_log = "./ctd_files_log.txt"

        with open(ctd_files_log, 'a+') as f:
            text = f"File is CTD at dataset id: {dataset_id}\n"
            f.write(text)

        # If the file is ctd, get geojson data if it exists
        geojson = get_geojson(dataset_id, processing_log)

        if geojson:            
            save_geojson(dataset_id, geojson)

            geojson_files_log = './geojson_ctd_files.txt'
            with open(geojson_files_log, 'a+') as f:
                text = f"File is geojson CTD at dataset id: {dataset_id}\n"
                f.write(text)  

    else:

        with open(processing_log, 'a+') as f:
            text = f"No ctd data for id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(text)

        return {}

    bco_dmo_json = create_bco_dmo_json(index, dataset_id, titles, investigators, dataset_soup, lon_lat_list)

    return bco_dmo_json


def main():

    # delete processing log if it exists
    processing_log = "./processing_log.txt"

    # if os.path.exists(processing_log):
    #     os.remove(processing_log)

    # Results start at page = 0
    search_start_page = 0
    number_of_datasets_per_page = 20

    page_dataset_ids, titles, investigators = meta.get_metadata(search_start_page)


    for index, (page, dataset_id) in enumerate(page_dataset_ids):

        # dataset 3458 and 527438 timed out. (Error 502) 
        # Not CTD file though
        if dataset_id in ['3458', '527438', '2467']:
        # if dataset_id in ['3355']:   
             continue
        else:
             pass

        print(f"Processing dataset id {dataset_id} on page {page + 1}: number {index+1} out of {len(page_dataset_ids)}")

        bco_dmo_json = get_bco_dmo_json(index, page, number_of_datasets_per_page, dataset_id, titles, investigators, processing_log)     

        if not bco_dmo_json:
            continue

        # Convert json from all lon/lat in one feature to one lon/lat per feature
        bco_dmo_geojson = convert_json_to_geojson(bco_dmo_json)

        bco_dmo_geojson = remove_duplicate_features(bco_dmo_geojson)

        # Save json to file
        #filename = str(dataset_id) + '-' + str(page) + '.json'
        filename = str(dataset_id) + '.json'

        filepath = './output/' + filename

        with open(filepath, 'w') as f:
            json.dump(bco_dmo_geojson, f, indent=4, sort_keys=True)  
 

if __name__ == '__main__':
    main()
    