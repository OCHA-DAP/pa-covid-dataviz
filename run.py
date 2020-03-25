import argparse
from os import getenv

import pandas as pd
from hdx.facades.keyword_arguments import facade
from hdx.hdx_configuration import Configuration
from hdx.location.country import Country
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file

import indicators
import timeseries
import cumulative

setup_logging()


OUTPUT_FILENAME = 'main.xlsx'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", "-d", action="store_true", help="Run without querying API")
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-db', '--db_url', default=None, help='Database connection string')
    parser.add_argument('-dp', '--db_params', default=None, help='Database connection parameters. Overrides --db_url.')
    parser.add_argument('-es', '--email_server', default=None, help='Email server to use')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    parser.add_argument('-et', '--email_test', default=False, action='store_true',
                        help='Email only test users for testing purposes')
    parser.add_argument('-st', '--spreadsheet_test', default=False, action='store_true',
                        help='Use test instead of prod spreadsheet')
    args = parser.parse_args()
    return args


def main(gsheet_auth, debug, **ignore):
    configuration = Configuration.read()
    countries = configuration['countries']
    palestine_country_code = Country.get_iso3_country_code('Palestine')
    df_indicators = indicators.create_dataframe(countries, debug)
    df_timeseries = timeseries.create_dataframe(countries, palestine_country_code, debug)
    df_cumulative = cumulative.create_dataframe(countries, palestine_country_code, debug)
    # Write to excel file
    writer = pd.ExcelWriter(OUTPUT_FILENAME, engine='xlsxwriter')
    df_indicators.to_excel(writer, sheet_name='Indicator', index=False)
    df_timeseries.to_excel(writer, sheet_name='Timeseries', index=False)
    df_cumulative.to_excel(writer, sheet_name='Cumulative', index=False)
    writer.save()


if __name__ == '__main__':
    args = parse_args()
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv('USER_AGENT')
        if user_agent is None:
            user_agent = 'test'
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv('PREPREFIX')
    preprefix = args.preprefix
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv('HDX_SITE', 'prod')
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv('GSHEET_AUTH')
    project_config_yaml = script_dir_plus_file('project_configuration.yml', main)
    facade(main, hdx_read_only=True, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site,
           project_config_yaml=project_config_yaml, gsheet_auth=gsheet_auth, debug=args.debug)
