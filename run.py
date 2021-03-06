import argparse
import json
import logging
import sys
from os import getenv
from os.path import join
from shutil import rmtree

import pygsheets
from google.oauth2 import service_account

import pandas as pd
from hdx.facades.keyword_arguments import facade
from hdx.hdx_configuration import Configuration
from hdx.location.country import Country
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import get_temp_dir

from model.main import get_indicators

setup_logging()
logger = logging.getLogger()


OUTPUT_FILENAME = 'main.xlsx'
VERSION = 1.3


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-f', '--folder', default=None, help='Download folder')
    parser.add_argument('-k', '--keep', action='store_true', help='Keep folder')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    parser.add_argument('-e', '--excel', action='store_true', help='Output to Excel not Google Sheets')
    parser.add_argument('-d', '--debug', action='store_true', help='Run without querying API')
    args = parser.parse_args()
    return args


def main(folder, keep, gsheet_auth, excel, debug, **ignore):
    logger.info('##### pa-covid-dataviz version %.1f ####' % VERSION)
    configuration = Configuration.read()
    countries = configuration['countries']
    palestine_country_code, _ = Country.get_iso3_country_code_fuzzy('Palestine')
    df_indicators, df_timeseries, df_cumulative = get_indicators(folder, countries, palestine_country_code, debug)
    if excel or gsheet_auth is None:
        # Write to excel file
        writer = pd.ExcelWriter(OUTPUT_FILENAME, engine='xlsxwriter')
        df_indicators.to_excel(writer, sheet_name='Indicator', index=False)
        df_timeseries.to_excel(writer, sheet_name='Timeseries', index=False)
        df_cumulative.to_excel(writer, sheet_name='Cumulative', index=False)
        writer.save()
    else:
        # Write to gsheets
        info = json.loads(gsheet_auth)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        gc = pygsheets.authorize(custom_credentials=credentials)
        spreadsheet = gc.open_by_url(configuration['spreadsheet_url'])
        sheet = spreadsheet.worksheet_by_title(configuration['indicator_sheetname'])
        sheet.clear()
        dfout = df_indicators.fillna('')
        sheet.set_dataframe(dfout, (1, 1))
        sheet = spreadsheet.worksheet_by_title(configuration['timeseries_sheetname'])
        sheet.clear()
        dfout = df_timeseries.fillna('')
        sheet.set_dataframe(dfout, (1, 1))
        sheet = spreadsheet.worksheet_by_title(configuration['cumulative_sheetname'])
        sheet.clear()
        dfout = df_cumulative.fillna('')
        sheet.set_dataframe(dfout, (1, 1))
    if not keep:
        rmtree(folder)


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
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv('HDX_SITE', 'prod')
    folder = args.folder
    if folder is None:
        folder = get_temp_dir('PA-COVID')
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv('GSHEET_AUTH')
    facade(main, hdx_read_only=True, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site,
           project_config_yaml=join('config', 'project_configuration.yml'), folder=folder, keep=args.keep,
           gsheet_auth=gsheet_auth, excel=args.excel, debug=args.debug)
