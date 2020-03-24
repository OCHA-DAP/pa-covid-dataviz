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
