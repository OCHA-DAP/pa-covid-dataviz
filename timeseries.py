import pandas as pd

import config
import utils


TIMESERIES_HDX_ADDRESS = 'novel-coronavirus-2019-ncov-cases'
TIMESERIES_DATASET_NAMES = {
    'confirmed': 'time_series-ncov-Confirmed.csv',
    'deaths': 'time_series-ncov-Deaths.csv',
    'recovered': 'time_series-ncov-Recovered.csv'}

POP_HDX_ADDRESS = 'world-bank-indicators-of-interest-to-the-covid-19-outbreak'
POP_DATASET_NAME = 'Total Population'

COLUMNS = ['Date', 'ISO3', 'Country', 'Active Cases', 'Total Deaths', 'Quart ile']


def create_dataframe(debug=False):
    # Download data and read in
    if not debug:
        filenames = utils.query_api(TIMESERIES_HDX_ADDRESS, dataset_names=list(TIMESERIES_DATASET_NAMES.values()))
        filename_pop = list(utils.query_api(POP_HDX_ADDRESS, [POP_DATASET_NAME]).values())[0]
    else:
        filenames = {filename: f'{filename}.CSV' for filename in TIMESERIES_DATASET_NAMES.values()}
        filename_pop = f'{POP_DATASET_NAME}.XLSX'
    # read it in
    confirmed = pd.read_csv(f'data/{filenames[TIMESERIES_DATASET_NAMES["confirmed"]]}')
    deaths = pd.read_csv(f'data/{filenames[TIMESERIES_DATASET_NAMES["deaths"]]}')
    recovered = pd.read_csv(f'data/{filenames[TIMESERIES_DATASET_NAMES["recovered"]]}')
    pop = pd.read_excel(f'data/{filename_pop}', sheet_name='Data', header=3)
    pop['Country Name'].loc[pop['Country Name'] == 'Syrian Arab Republic'] = 'Syria'
    pop['Country Name'].loc[pop['Country Name'] == 'Venezuela, RB'] = 'Venezuela'
    # process and merge timeseries data
    timeseries = pd.DataFrame()
    timeseries = timeseries.append(create_timeseries(confirmed, 'confirmed'))
    timeseries = timeseries.merge(create_timeseries(deaths, 'total deaths'), on=['Country', 'Date'], how='left')
    timeseries = timeseries.merge(create_timeseries(recovered, 'recovered'), on=['Country', 'Date'], how='left')
    pop_data = get_pop_data(pop)
    timeseries = timeseries.merge(pop_data[['Country Name', 'Country Code', 'latest population']], left_on='Country',
                                  right_on='Country Name', how='left')
    timeseries = timeseries.drop('Country Name', axis=1)
    timeseries['pop 100000'] = timeseries['latest population']/100000
    timeseries['active cases'] = timeseries['confirmed'] - (timeseries['total deaths']+timeseries['recovered'])
    timeseries['active cases per 100000'] = timeseries['active cases']/timeseries['pop 100000']
    timeseries = get_ranks(timeseries)
    print(timeseries.head())
    timeseries.to_csv("OCHA_cases_timeseries.csv", index=False)
    return timeseries


def create_timeseries(indicator_df, col_name):
    # subset data
    country_data = indicator_df.loc[indicator_df['Country/Region'].isin(config.countries)==True]
    country_data = country_data[['Country/Region', 'Date', 'Value']]
    country_data['Value'] = country_data['Value'].astype(int)
    country_data.columns = ['Country', 'Date', col_name]
    return country_data


def get_pop_data(data):
    # apply latest year to population data
    data_current = pd.DataFrame()
    for c in config.countries:
        year = utils.get_latest_year(data, c)
        current_c = data[data['Country Name']==c][['Country Name', 'Country Code', year]]
        current_c.columns = ['Country Name', 'Country Code', 'latest population']
        data_current = data_current.append(current_c)
    return data_current


def get_quartile(rank):
    # convert percentiles to quartiles
    if rank >= 0.75:
        return 1
    elif rank >= 0.5:
        return 2
    elif rank >= 0.25:
        return 3
    else:
        return 4


def get_ranks(df):
    # get the ranks for each 7 day window
   new_df = pd.DataFrame()
   for country in df['Country'].unique():
       sub = df.loc[df['Country'] == country]
       sub['prev active cases'] = sub['active cases per 100000'].shift(-7, axis=0)
       new_df = new_df.append(sub)
   new_df['delta'] = new_df['active cases per 100000']-new_df['prev active cases']
   ranked_df = pd.DataFrame()
   for date in new_df['Date'].unique():
       date_sub = new_df.loc[new_df['Date']==date] 
       date_sub['rank'] = date_sub['delta'].rank(pct=True)
       ranked_df = ranked_df.append(date_sub)
   ranked_df['quartile'] = ranked_df['rank'].apply(lambda x: get_quartile(x))
   return ranked_df
