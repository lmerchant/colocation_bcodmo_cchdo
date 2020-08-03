import requests
import json
from datetime import timedelta, datetime


def save_modified_geojson(dataset_id, geojson):
    # Save json to file
    filename = str(dataset_id) + '.json'

    filepath = './output_erddap_modified_geojson_jsonld_dates/' + filename

    with open(filepath, 'w') as f:
        json.dump(geojson, f, indent=4, sort_keys=True) 


def split_geojson_by_cruise_id(geojson):

    features = geojson['features']

    cruiseid_set = set()

    for feature in features:

        properties = feature['properties']
        cruiseid = properties['cruiseid']

        cruiseid_set.add(cruiseid)

    print(cruiseid_set)

    return geojson


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


def get_temporal_coverage(dataset_soup):

    # Find "temporalCoverage": "1999-03-29/1999-06-28" if exists in html

    try:
        data = dataset_soup.select("[type='application/ld+json']")[1]

        temporal_coverage = json.loads(data.text)["temporalCoverage"]

        return temporal_coverage

    except:

        return None


def get_temporal_extent(dataset_soup):

    # dataset-temporal-bounds

    # lmm, What if only one datein temporal bounds. Then end date is same a start date

    try:
        temporal_extent = dataset_soup.find("div", {"id": "dataset-temporal-bounds"})

        # Temporal Extent: 1988-10-30 - 2016-11-28
        parts = temporal_extent.split(' ')
        start_date = parts[-3]
        end_date = parts[-1]

    except:
        start_date = None
        end_date = None

    return start_date, end_date


def get_start_date(dataset_soup):

    # <td class="views-field views-field-field-deployment-start-date nowrap" >
    #<span class="date-display-single" property="dc:date" datatype="xsd:dateTime" content="1989-06-28T04:00:00-04:00">1989-06-28</span>          
    #</td>
    try: 
        td = dataset_soup.find('td', {'class': 'views-field-field-deployment-start-date'})
        span = td.find('span')
        start_date = span.string
    except:
        start_date = None

    return start_date


def get_start_end_dates_from_json_ld(dataset_soup):

    start_date = None
    end_date = None

    # See if json+ld exists for page and grab temporal coverage
    temporal_coverage = get_temporal_coverage(dataset_soup)

    if temporal_coverage:

        dates = temporal_coverage.split('/')

        start_date = dates[0]
        end_date = dates[1]

    return start_date, end_date


def get_start_end_dates(dataset_soup):

    start_date = None
    end_date = None

    # First get start date from deployments section of site
    start_date1 = get_start_date(dataset_soup)

    # Also try to get temporal extent from map area of site
    start_date2, end_date2 = get_temporal_extent(dataset_soup)

    if not start_date2:
        start_date = start_date1

    if end_date2:
        end_date = end_date2

    # See if json+ld exists for page and grab temporal coverage
    temporal_coverage = get_temporal_coverage(dataset_soup)

    if temporal_coverage:

        dates = temporal_coverage.split('/')

        start_date = dates[0]
        end_date = dates[1]

    return start_date, end_date


def convert_yr_mon_day_to_date(year, month, day):

    date = f"{str(year)}-{str(month)}-{str(day)}"

    return date


def convert_decimal_year_to_date(year, yrday_local):

    day_one = datetime(year,1,1)
    days_datetime = timedelta(yrday_local)
    date = day_one + days_datetime

    return date.strftime('%Y-%m-%d')


def modify_geojson_attributes(dataset_id, start_date, end_date, geojson):
    # Remove some attributes and include dataset id as attribute

    has_cruiseid = False

    del geojson['bbox']
    del geojson['propertyNames']
    del geojson['propertyUnits']    

    features = geojson['features']

    for feature in features:

        properties = feature['properties']

        new_properties = {}
        
        try:
            new_properties['cruiseid'] = properties['cruiseid']

            #has_cruiseid = True

        except KeyError:
            pass

        new_properties['dataset_id'] = dataset_id

        new_properties['start_date'] = start_date
        new_properties['end_date'] = end_date

        # try:
        #     year = properties['year']
        #     yrday_local = properties['yrday_local']            
        #     time_local = properties['time_local']
            
        #     date = convert_decimal_year_to_date(year, yrday_local)

        #     new_properties['date'] = date

        # except KeyError:
        #     pass

        # try:
        #     year = properties['year']
        #     month = properties['month_gmt']
        #     day = properties['day_gmt']
        #     time = properties['time_start_gmt']

        #     date = convert_yr_mon_day_to_date(year, month, day)

        #     new_properties['date'] = date

        # except KeyError:
        #     pass

    
        feature['properties'] = new_properties

    #return geojson, has_cruiseid
    return geojson


def write_to_geojson_files_log(dataset_id, geojson_files_log):
    with open(geojson_files_log, 'a+') as f:
        text = f"File is geojson CTD at dataset id: {dataset_id}\n"
        f.write(text) 


def save_geojson(dataset_id, geojson):

    # Save json to file
    filename = str(dataset_id) + '.json'

    filepath = './output_geojson/' + filename

    with open(filepath, 'w') as f:
        json.dump(geojson, f, indent=4, sort_keys=True) 


def get_geojson_url(dataset_id):

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

