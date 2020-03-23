import shutil
import os
import logging

import pandas as pd
from hdx.hdx_configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging

import config


HDX_SITE = 'prod'
USER_AGENT = 'A_Quick_Example'
WORLD_BANK_DATASET = 'world-bank-indicators-of-interest-to-the-covid-19-outbreak'
COLUMNS = ['Indicator', 'ISO3', 'Country', 'Value', 'Last Updated']

DATASETS = [
    'Age and Population - Population ages 65 and above (% of total)',
    'Water & Sanitation - People with basic handwashing facilities including soap and water (% of population)',
    # 'Estimated number of people living with HIV - Adult (>15) rate',
    # 'Number of people in need',
    'Health - Hospital beds (per 1,000 people)',
    'Health - Physicians (per 1,000 people)',
    'Health - Nurses and midwives (per 1,000 people)',
    'Health - Out-of-pocket expenditure per capita, PPP (current international $)'
]


setup_logging()
logger = logging.getLogger()


def create_dataframe():
    df = make_initial_dataframe()
    filenames = query_api()


def make_initial_dataframe():
    df = pd.DataFrame(columns=COLUMNS)
    return df


def query_api():
    Configuration.create(hdx_site=HDX_SITE, hdx_read_only=True, user_agent=USER_AGENT)
    dataset = Dataset.read_from_hdx(WORLD_BANK_DATASET)
    resources = dataset.get_resources()
    filenames = {}
    for resource in resources:
        if resource['name'] in DATASETS:
            print(resource)
            _, path = resource.download()
            filename = os.path.basename(path)
            shutil.move(path, f'data/{filename}')
            filenames[resource['name']] = filename
            logging.info(f'Saved {resource["name"]} to data/{filename}')
    return filenames


if __name__ == "__main__":
    create_dataframe()
