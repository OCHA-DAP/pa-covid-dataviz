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


def create_dataframe(debug=True):
    df_main = make_initial_dataframe()
    if not debug:
        filenames = query_api()
    else:
        filenames = {d: f'{d}.XLSX' for d in DATASETS}
    for indicator, filename in filenames.items():
        data_current = extract_data_from_excel(f'data/{filename}')
        data_current['Indicator'] = indicator
        df_main = df_main.append(data_current)


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
            _, path = resource.download()
            filename = os.path.basename(path)
            shutil.move(path, f'data/{filename}')
            filenames[resource['name']] = filename
            logging.info(f'Saved {resource["name"]} to data/{filename}')
    return filenames


def extract_data_from_excel(excel_path):
    output_columns = ['Country', 'ISO3', 'Value', 'Last Updated']
    data = pd.read_excel(excel_path, sheet_name='Data', header=3)
    data_current = pd.DataFrame(columns=output_columns)
    for c in config.countries:
        year = get_latest_year(data, c)
        current_c = data[data['Country Name'] == c][['Country Name', 'Country Code', year]]
        current_c['year'] = year
        current_c.columns = output_columns
        data_current = data_current.append(current_c)
    global_vals = []
    for c in data['Country Name'].unique():
        year = get_latest_year(data, c)
        val = data.loc[data['Country Name'] == c][year].values[0].astype(str)
        if val != 'nan':
            global_vals = global_vals+[data.loc[data['Country Name']==c][year].tolist()[0]]
    global_baseline = sum(global_vals)/len(global_vals)
    data_current = data_current.append(
        pd.DataFrame({'Country': 'Global', 'ISO3': 'Global',
                      'Value': global_baseline}, index=[0]))
    return data_current


def get_latest_year(df, country, start_year=2019, min_year=2009):
    country_row = df.loc[df['Country Name'] == country]
    y = start_year
    while y > min_year:
        try:
            if country_row[str(y)].values[0].astype(str) != 'nan':
                return str(y)
        except IndexError:
            pass
        y -= 1
    return str(min_year)


if __name__ == "__main__":
    create_dataframe()
