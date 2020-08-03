import requests
from bs4 import BeautifulSoup

import utilities.erddap_get_dataset_ids as ds
import utilities.erddap_check_if_ctd as ctd
import utilities.erddap_check_if_ctd_jsonld as ctd_jsonld
import utilities.modify_geojson as geo



def get_dataset_url(dataset_id):

    url = f'http://www.bco-dmo.org/dataset/{dataset_id}'
    return url  


def get_dataset_soup(dataset_id):

    url = get_dataset_url(dataset_id)

    try:
        response = requests.get(url)
        dataset_soup = BeautifulSoup(response.text, 'html.parser')

    except requests.exceptions.RequestException as e:
        # response doesn't exist
        dataset_soup = None

        # # Can't reach data page
        # # Log it
        # with open(processing_log, 'a+') as f:
        #     text = f"Site not reached for dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
        #     f.write(text)

    # No dataset available
    if dataset_soup and not dataset_soup(text='Data URL:'):

        # with open(processing_log, 'a+') as f:
        #     text = f"No data set for id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
        #     f.write(text)

        dataset_soup = None

    return dataset_soup


def main():

    # delete processing log if it exists
    processing_log = "./geojson_processing_log_tmp.txt"

    geojson_files_log = "./geojson_files_log_tmp.txt"

    # if os.path.exists(processing_log):
    #     os.remove(processing_log)

    
    # Get list of CTD dataset ids from ERDAPP page
    dataset_ids = ds.get_ctd_dataset_ids()

    for dataset_id in dataset_ids:

        #dataset_id = '3937'

        # check if ctd type
        print(f"Processing dataset id {dataset_id}")

        #is_ctd = ctd.check_if_ctd(dataset_id, processing_log)
        is_ctd = ctd_jsonld.check_if_ctd(dataset_id, processing_log)

        # Assume all are ctd
        #is_ctd = True

        if is_ctd:
            
            # If the file is ctd, get geojson data
            geojson = geo.get_geojson(dataset_id, processing_log)

            if geojson:
                geo.save_geojson(dataset_id, geojson)
                geo.write_to_geojson_files_log(dataset_id, geojson_files_log) 
            else:
                continue

            # Get html soup of dataset web page 
            dataset_soup = get_dataset_soup(dataset_id)             
            
            # Get temporal coverage from json+ld or 
            # dataset web page
            if dataset_soup:
                #start_date, end_date = geo.get_start_end_dates(dataset_soup)
                start_date, end_date = geo.get_start_end_dates_from_json_ld(dataset_soup)
            else:
                start_date = None
                end_date = None

            print('start_date', start_date)
            print('end_date', end_date)

            # modify geoJSON attributes
            # Remove some attributes and include dataset id as attribute            
            geojson = geo.modify_geojson_attributes(dataset_id, start_date, end_date, geojson)

            geojson = geo.remove_duplicate_features(geojson)

            # Save geoJSON to file
            geo.save_modified_geojson(dataset_id, geojson)           

        else:
            with open(processing_log, 'a+') as f:
                text = f"File is not CTD at dataset id: {dataset_id}\n"
                f.write(text)


if __name__ == '__main__':
    main()
