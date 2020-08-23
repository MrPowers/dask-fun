import dask.dataframe as dd

def convert_csvs_to_parquet():
    df = dd.read_csv('./data/people/*.csv')
    df.to_parquet('./tmp/people_parquet2')
