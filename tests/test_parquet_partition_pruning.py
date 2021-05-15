# import pytest
# import dask.dataframe as dd
#
# def test_performs_parquet_predicate_pushdown_pruning():
#     df = dd.read_csv('./data/pets/*.csv')
#     assert df.npartitions == 4
#     df.to_parquet('./tmp/pets_parquet', write_index=False)
#     ddf = dd.read_parquet('./tmp/pets_parquet', filters=[('age', '>', 10)])
#     assert ddf.npartitions == 1
#
