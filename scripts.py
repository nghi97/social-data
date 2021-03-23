import os
import queries
import pandas as pd


def fix_chmura_counties():
    counties = queries.counties_query()
    counties.set_index(['State', 'County Name'], inplace=True)
    ch_df = queries.generic_select_query('chmura_economic_vulnerability_index',
                                         ['fips', 'name', 'VulnerabilityIndex', 'Rank', 'state', 'county_id'])
    print(ch_df.shape)
    for i, row in ch_df.iterrows():
        if pd.isnull(row['county_id']):
            try:
                ch_df.at[i, 'county_id'] = counties.loc[row['state'], row['name']]['county_id']
            except KeyError:
                print(row['state'], row['name'])
        # print(ch_df.loc[row['state'],row['name']])
    print(ch_df.shape)
    print(ch_df.head())
    queries.write_table(ch_df, 'chmura_economic_vulnerability_index')
    return


def import_data(data_dir: str):
    for root, dirs, files in os.walk(data_dir, topdown=False):
        for file in files:
            if '.csv' in file:
                print(file)
                try:
                    data = pd.read_csv(os.path.join(root, file))
                    name = file[:-4]
                    data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
                    queries.write_table(data, name)
                    print(file+' uploaded')
                except Exception as e:
                    print(e)
    return


if __name__ == '__main__':
    # fix_chmura_counties()
    import_data('temp/')
