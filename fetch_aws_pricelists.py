# Copyright 2010-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This file is licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""
This script downloads the list of price lists from the AWS Pricing API and consolidates all price lists in a single CSV
file.
You can configure several parameters at the bottom of this script, right after the 'main' section.
Further documentation:
* https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/using-the-aws-price-list-bulk-api-fetching-price-list-files.html
* https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/reading-service-price-list-file-for-services.html
"""
import boto3
from botocore.exceptions import ClientError
import pprint
from datetime import datetime, UTC
import requests
from requests.exceptions import ConnectionError
import os
import pandas as pd
import concurrent.futures
import time
import json


client = boto3.client('pricing', region_name='eu-central-1')
pp = pprint.PrettyPrinter(indent=4)


def describe_services(store_as_json=False):
    """
    Lists all the AWS services information
    :param bool store_as_json: If True a json file will be writen in the local folder
    :return list: List of services information
    """
    paginator = client.get_paginator('describe_services')
    params = {}
    page_iterator = paginator.paginate(**params)
    services = []
    for page in page_iterator:
        services += page['Services']
    if store_as_json is True:
        filename = "aws_services_list.json"
        payload = [service['ServiceCode'] for service in services]
        json.dump(payload, open(filename, "w"))
        print("List of {} AWS services stored in your local folder under the name {}.".format(len(payload), filename))
    return services


def list_price_list(service_code, region, currency, date):
    """
    List all the price lists for the given arguments
    :param str service_code: AWS service identification code
    :param str region: AWS region
    :param str currency: current
    :param datetime date: validity date
    :return list: the price lists
    """
    count = 0
    max_retry = 3
    while True:
        try:
            price_lists = []
            paginator = client.get_paginator('list_price_lists')
            params = {
                'ServiceCode': service_code,
                'EffectiveDate': date,
                'RegionCode': region,
                'CurrencyCode': currency}
            page_iterator = paginator.paginate(**params)
            for page in page_iterator:
                for price_list in page['PriceLists']:
                    price_list['ServiceCode'] = service_code
                    price_lists.append(price_list)
            return price_lists
        except ClientError:
            # Wait a bit before retry
            if count > max_retry:
                raise
            else:
                count += 1
                time.sleep(2 * count)


def get_price_list_url(price_list_arn, file_format='csv'):
    """
    Retrieve the URL to download a price list
    :param str price_list_arn: the arn of the price list
    :param str file_format: 'csv' or 'json'
    :return:
    """
    count = 0
    max_retry = 3
    while True:
        try:
            resp = client.get_price_list_file_url(
                PriceListArn=price_list_arn,
                FileFormat=file_format
            )
            # pp.pprint(resp)
            return resp.get('Url')
        except ClientError:
            # Wait a bit before retry
            if count > max_retry:
                raise
            else:
                count += 1
                time.sleep(2 * count)


def get_price_list_as_json(url, timeout=2, retry=3):
    """
    Download a tariff document in JSON format for the passed url
    :param url url: The url to fetch the doc
    :param retry: max number of retries to get the list (attempts = retry + 1)
    :param timeout: timeout for http request
    :return dict: JSON document
    """
    headers = {'Accept': 'application/json'}
    count = 0
    max_retry = retry
    while True:
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            return r.json()
        except ConnectionError:
            # Wait a bit before retry
            if count > max_retry:
                raise
            else:
                count += 1
                time.sleep(2 * count)


def get_price_list_as_csv(url, timeout=2, retry=3):
    """
    Download a tariff document in CSV format for the passed url
    :param url url: The url to fetch the doc
    :param retry: max number of retries to get the list (attempts = retry + 1)
    :param timeout: timeout for http request
    :return str: decoded document
    """
    count = 0
    max_retry = retry
    while True:
        try:
            r = requests.get(url, timeout=timeout)
            return r.content.decode('utf-8')
        except ConnectionError:
            # Wait a bit before retry
            if count > max_retry:
                raise
            else:
                count += 1
                time.sleep(2 * count)


def store_raw_price_list(pair, raw_csv_dir, currency, date):
    """
     Fetch all price lists for a given region and service, and store as-is in CSV format on disk
    :param dict pair: {'region', 'service'}
    :param str raw_csv_dir: directory where to store the raw tariff file
    :param str currency: currency to fetch
    :param datetime date: validity date of the tariff
    :return int: number of tariff lists fetched
    """
    region = pair['region']
    service = pair['service']
    price_lists = list_price_list(
        service_code=service,
        region=region,
        currency=currency,
        date=date
    )
    for price_list in price_lists:
        price_list['Url'] = get_price_list_url(price_list['PriceListArn'], file_format='csv')
    print("Got {} Price Lists for {} in region {}".format(len(price_lists), service, region))
    count = 0
    for price_list in price_lists:
        count += 1
        file_path = os.path.join(raw_csv_dir, "price_list_{}_{}_raw_{}.csv".format(service, region, count))
        with open(file_path, "w") as f:
            f.write(get_price_list_as_csv(price_list['Url']))
    return count


def store_raw_price_lists(services_included, services_excluded, raw_csv_dir, regions, currency, date, nb_workers=10):
    """
    Threaded job collecting all the tariff lists.
    :param set services_included: Services to include
    :param set services_excluded: Services to exclude
    :param str raw_csv_dir: location of the raw CSV price list files
    :param set regions: list of regions to fetch
    :param str currency: currency to use
    :param datetime date: validity date of the price list
    :param int nb_workers: Number of threads to launch - avoid increasing due to Throttling by the API
    :return: None
    """
    print("\nStating to fetch price lists")
    all_services = {service['ServiceCode'] for service in describe_services()}
    if services_included:
        service_codes = all_services.intersection(services_included)
    elif services_excluded:
        service_codes = all_services.difference(services_excluded)
    else:
        service_codes = all_services
    # pp.pprint(service_codes)
    os.makedirs(raw_csv_dir, exist_ok=True)
    pairs = []
    for region in regions:
        for service in service_codes:
            pairs.append({'region': region, 'service': service})
    count = 0
    count_lists = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=nb_workers) as executor:
        future_uploads = {executor.submit(store_raw_price_list, pair, raw_csv_dir, currency, date): pair for pair in
                          pairs}
        for future in concurrent.futures.as_completed(future_uploads):
            count += 1
            count_lists += future.result()
            print("{} pairs (region, service) processed.".format(count), end="\r")
    if count_lists > 0:
        print("\n")
        print("{} price lists found".format(count_lists))
    else:
        print("!!! WARNING: No price list found !!!\n")


def truncate_raw_list(raw_csv_dir, truncated_csv_dir, used_headers):
    """
    Eliminate the unused columns in the raw CSV files and store in a different directory
    :param str raw_csv_dir: location of the raw CSV price list files
    :param str truncated_csv_dir: location of the CSV price list files with unused columns removed
    :param set used_headers: a set of price list properties to collect
    :return: None
    """
    print("Starting truncating CSV files")
    os.makedirs(truncated_csv_dir, exist_ok=True)
    count = 0
    for f in os.listdir(raw_csv_dir):
        source_path = os.path.join(raw_csv_dir, f)
        trunc_path = os.path.join(truncated_csv_dir, f.replace("raw", "trunc"))
        if os.path.isfile(source_path) and f.endswith(".csv"):
            count += 1
            data = pd.read_csv(source_path,
                               skiprows=5,
                               dtype=str)
            headers = set(data.columns)
            discard = headers.difference(used_headers)
            data.drop(labels=list(discard), axis=1, inplace=True)
            data.to_csv(trunc_path, index=False)
            print("Truncated files: {}".format(count), end='\r')
    if not count > 0:
        print("!!! WARNING: No price list found to truncate !!!")
    print("")


def consolidate_all_tariffs(truncated_csv_dir, consolidated_csv_dir, date):
    """
    Regroups all the CSV files in a single one using threads to go faster
    :param str truncated_csv_dir:  location of the CSV price list files with unused columns removed
    :param str consolidated_csv_dir: location of the CSV price list files including all the data collected
    :param datetime date: validity date of the price lists
    :return: None
    """
    print("Starting consolidation of truncated files")
    os.makedirs(consolidated_csv_dir, exist_ok=True)
    tariffs = []
    for f in os.listdir(truncated_csv_dir):
        trunc_path = os.path.join(truncated_csv_dir, f)
        if os.path.isfile(trunc_path) and f.endswith(".csv"):
            tariffs.append(pd.read_csv(trunc_path))
            print("Consolidation: {}".format(len(tariffs)), end='\r')
    consolidated_path = os.path.join(consolidated_csv_dir, "aws-tariffs-{}.csv".format(date.strftime("%y-%m-%d")))
    if tariffs:
        print("")
        pd.concat(tariffs, ignore_index=True).to_csv(consolidated_path, index=False)
        print("{} price lists consolidate  in a single document".format(len(tariffs)))
    else:
        print("!!! WARNING: no price list found to concatenate !!!")


def get_all_regions():
    """
    Fetch a list of all available regions in your account.
    Will not include:
    * China: cn-north-1, cn-northwest-1 -
    * Government Cloud: us-gov-east-1, us-gov-west-1)
    :return: a sorted list of AWS Region codes
    """
    rclient = boto3.client('account', region_name='eu-central-1')
    paginator = rclient.get_paginator('list_regions')
    page_iterator = paginator.paginate()
    regions_info = []
    for page in page_iterator:
        regions_info += page['Regions']
    return sorted([r['RegionName'] for r in regions_info])


if __name__ == '__main__':
    '''CONFIGURATION SECTION STARTS HERE'''
    # Documents storage locations
    RAW_CSV_DIR = "raw_csv"
    TRUNCATED_CSV_DIR = "truncated_csv"
    CONSOLIDATED_CSV_DIR = "consolidated_csv"

    # The properties of the price lists to collect. Those will be headers of the Consolidated CSV
    USED_HEADERS = {"SKU", "PriceDescription", "Unit", "RateCode", "serviceCode", "serviceName", "Product Family",
                    "Location", "Location Type", "usageType", "PricePerUnit"}

    # Validity date for the price lists
    DATE = datetime.now(UTC)
    # What currency to fetch price lists for
    CURRENCY = 'USD'

    # The services to consider
    SERVICES_INCLUDED = set()  # If empty all the services in the regions will be fetched
    # Example how to limit the number of services fetched
    SERVICES_INCLUDED.update(['awskms', 'AmazonEC2'])
    SERVICES_EXCLUDED = set()  # Only used if services_included is empty
    # Example how to exclude some of the services.
    # SERVICES_EXCLUDED.update(['awskms', 'AmazonEC2'])

    """
    List of all available regions as of 2024/01/23 
    ['af-south-1', 'ap-east-1', 'ap-northeast-1', 'ap-northeast-2', 'ap-northeast-3', 'ap-south-1', 'ap-south-2', 
    'ap-southeast-1', 'ap-southeast-2', 'ap-southeast-3', 'ap-southeast-4', 'ca-central-1', 'ca-west-1', 'eu-central-1', 
    'eu-central-2', 'eu-north-1', 'eu-south-1', 'eu-south-2', 'eu-west-1', 'eu-west-2', 'eu-west-3', 'il-central-1', 
    'me-central-1', 'me-south-1', 'sa-east-1', 'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
    
    Not included: China (cn-north-1, cn-northwest-1) and Government Cloud (us-gov-east-1, us-gov-west-1)
    """
    # You can get a fresh list of all the regions for your account with:
    # REGIONS = get_all_regions()
    # Regions to use: leave empty set for all available regions
    REGIONS_INCLUDED = set()
    # Example how to limit the number of regions fetched
    REGIONS_INCLUDED.update(['us-east-1', 'eu-central-1'])
    # Regions to exclude: only considered if not empty and REGIONS_INCLUDED is empty
    REGIONS_EXCLUDED = set()
    # Example how to exclude some of the services.
    # REGIONS_EXCLUDED.update(['ap-northeast-1'])

    # What to do
    STORE_AWS_SERVICES_CODES_AS_JSON = False
    FETCH_RAW_PRICE_LISTS = True
    TRUNCATE_RAW_PRICE_LISTS = True
    CONSOLIDATE_TRUNCATED_PRICE_LISTS = True
    '''CONFIGURATION SECTION ENDS HERE'''

    # Normalise the paths
    RAW_CSV_DIR = os.path.normpath(RAW_CSV_DIR)
    TRUNCATED_CSV_DIR = os.path.normpath(TRUNCATED_CSV_DIR)
    CONSOLIDATED_CSV_DIR = os.path.normpath(CONSOLIDATED_CSV_DIR)

    # Build a list for regions to process
    REGIONS = set(get_all_regions())
    if REGIONS_INCLUDED:
        REGIONS = REGIONS.intersection(REGIONS_INCLUDED)
    elif REGIONS_EXCLUDED:
        REGIONS = REGIONS.difference(REGIONS_EXCLUDED)

    if STORE_AWS_SERVICES_CODES_AS_JSON is True:
        describe_services(True)

    if FETCH_RAW_PRICE_LISTS is True:
        store_raw_price_lists(services_included=SERVICES_INCLUDED,
                              services_excluded=SERVICES_EXCLUDED,
                              raw_csv_dir=RAW_CSV_DIR,
                              regions=REGIONS,
                              currency=CURRENCY,
                              date=DATE)

    if TRUNCATE_RAW_PRICE_LISTS is True:
        truncate_raw_list(raw_csv_dir=RAW_CSV_DIR,
                          truncated_csv_dir=TRUNCATED_CSV_DIR,
                          used_headers=USED_HEADERS)

    if CONSOLIDATE_TRUNCATED_PRICE_LISTS is True:
        consolidate_all_tariffs(truncated_csv_dir=TRUNCATED_CSV_DIR,
                                consolidated_csv_dir=CONSOLIDATED_CSV_DIR,
                                date=DATE)
    print("\nGoodbye!")
