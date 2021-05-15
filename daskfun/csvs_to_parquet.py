import dask.dataframe as dd


def convert_csvs_to_parquet2():
    df = dd.read_csv("./data/people/*.csv")
    df.to_parquet("./tmp/people_parquet2", write_index=False)


def convert_csvs_to_parquet4():
    df = dd.read_csv("./data/people/*.csv")
    df = df.repartition(npartitions=4)
    df.to_parquet("./tmp/people_parquet4", write_index=False)


def convert_csvs_to_snappy_parquet():
    df = dd.read_csv("./data/people/*.csv")
    df.to_parquet(
        "./tmp/people_compressed_parquet", write_index=False, compression="snappy"
    )
