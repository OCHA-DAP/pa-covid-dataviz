from os.path import join

import pandas as pd

from model import utils

TIMESERIES_HDX_ADDRESS = 'coronavirus-covid-19-cases-data-for-china-and-the-rest-of-the-world'
TIMESERIES_DATASET_NAME = 'covid-19 historical cases by country.csv'

POP_HDX_ADDRESS = 'world-bank-indicators-of-interest-to-the-covid-19-outbreak'
POP_DATASET_NAME = 'Total Population'

COLUMNS = ['Date', 'ISO3', 'Country', 'Active Cases', 'Total Deaths', 'Quart ile']


def create_dataframe(folder, countries, palestine_country_code, debug=False):
    # Download data and read in
    if not debug:
        filename = list(utils.query_api(folder, TIMESERIES_HDX_ADDRESS, [TIMESERIES_DATASET_NAME]).values())[0]
        filename_pop = list(utils.query_api(folder, POP_HDX_ADDRESS, [POP_DATASET_NAME]).values())[0]
    else:
        filename = f'{TIMESERIES_DATASET_NAME}.CSV'
        filename_pop = f'{POP_DATASET_NAME}.XLSX'
    # read them in
    case_data = pd.read_csv(join(folder, filename))
    case_data['ADM0_NAME'] = case_data['ADM0_NAME'].str.lower().replace({
            'Syrian Arab Republic': 'Syria',
            'venezuela (bolivarian republic of)': 'Venezuela',
         })
    pop = pd.read_excel(join(folder, filename_pop), sheet_name='Data', header=3)
    pop['Country Name'] = pop['Country Name'].replace({
                                                       'Syrian Arab Republic': 'Syria',
                                                       'Venezuela, RB': 'Venezuela',
                                                       'Congo, Dem. Rep.': 'Democratic Republic of the Congo'
                                                       })
    # process and merge timeseries data
    timeseries = pd.DataFrame()
    timeseries = timeseries.append(create_timeseries(countries, case_data, 'confirmed cases'))
    pop_data = utils.get_pop_data(countries, pop)
    timeseries = timeseries.merge(pop_data[['Country Name', 'Country Code', 'latest population']], left_on='Country',
                                  right_on='Country Name', how='left')
    timeseries = timeseries.drop('Country Name', axis=1)
    timeseries['Country Code'].loc[timeseries['Country']
                                   == 'Occupied Palestinian Territory'] = palestine_country_code
    timeseries['pop 100000'] = timeseries['latest population']/100000
    timeseries['confirmed cases per 100000'] = timeseries['confirmed cases']/timeseries['pop 100000']
    timeseries = get_ranks(timeseries)
    print(timeseries.head())
    return timeseries


def create_timeseries(countries, indicator_df, col_name):
    # subset data
    country_data = indicator_df.loc[indicator_df['ADM0_NAME'].str.lower()
                                    .isin(country_name.lower() for country_name in countries)]
    country_data = country_data[['ADM0_NAME', 'DateOfDataEntry', 'cum_conf']]
    country_data['cum_conf'] = country_data['cum_conf'].astype(int)
    country_data.columns = ['Country', 'Date', col_name]
    country_data['Country'] = country_data['Country'].str.split().apply(
        lambda x: [el.capitalize() if el not in ['of', 'the'] else el for el in x]).str.join(' ')

    return country_data


def get_quartile(rank):
    # convert percentiles to quartiles
    if rank >= 0.75:
        return 1
    elif rank >= 0.5:
        return 2
    elif rank >= 0.25:
        return 3
    elif rank >= 0:
        return 4
    else:
        return None


def get_ranks(df):
    # get the ranks for each 7 day window
    new_df = pd.DataFrame()
    df = df.sort_values(by='Date', ascending=False)
    for country in df['Country'].unique():
        sub = df.loc[df['Country'] == country]
        sub['prev confirmed cases'] = sub['confirmed cases per 100000'].shift(-7, axis=0)
        new_df = new_df.append(sub)
    new_df['delta'] = new_df['confirmed cases per 100000']-new_df['prev confirmed cases']
    ranked_df = pd.DataFrame()
    for date in new_df['Date'].unique():
        date_sub = new_df.loc[new_df['Date'] == date]
        date_sub['rank'] = date_sub['delta'].rank(pct=True)
        ranked_df = ranked_df.append(date_sub)
    ranked_df['quartile'] = ranked_df['rank'].apply(lambda x: get_quartile(x))
    return ranked_df
