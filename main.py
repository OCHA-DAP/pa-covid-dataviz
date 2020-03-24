import argparse

import pandas as pd

import indicators
import timeseries

OUTPUT_FILENAME = 'main.xlsx'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", "-d", action="store_true", help="Run without querying API")
    args = parser.parse_args()
    return args


def main(debug):
    #df_indicators = indicators.create_dataframe(debug)
    df_timeseries = timeseries.create_dataframe(debug)
    # Write to excel file
    writer = pd.ExcelWriter(OUTPUT_FILENAME, engine='xlsxwriter')
    #df_indicators.to_excel(writer, sheet_name='Indicator', index=False)
    df_timeseries.to_excel(writer, sheet_name='Timeseries', index=False)
    writer.save()


if __name__ == "__main__":
    args = parse_args()
    main(debug=args.debug)
