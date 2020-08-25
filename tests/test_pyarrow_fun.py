import pytest

import pyarrow.csv as pv
import pyarrow.parquet as pq


def test_pyarrow_compression():
    table = pv.read_csv('./data/people/people1.csv')
    pq.write_table(table, './tmp/pyarrow_out/people1.parquet')
    parquet_file = pq.ParquetFile('./tmp/pyarrow_out/people1.parquet')
    # print(parquet_file.metadata)
    # print(parquet_file.metadata.row_group(0))
    # print(parquet_file.metadata.row_group(0).column(0))
    # print(parquet_file.metadata.row_group(0).column(0).statistics)
    assert parquet_file.metadata.row_group(0).column(0).compression == 'SNAPPY'
    # parquet_file = pq.ParquetFile('example.parquet')
    # parquet_file = pq.ParquetFile('./tmp/people_parquet2/part.0.parquet')
    # parquet_file = pq.ParquetFile('./tmp/people_compressed_parquet/part.0.parquet')
    # parquet_file.metadata
    # parquet_file.schema


def test_pyarrow_statistics():
    table = pv.read_csv('./data/pets/pets1.csv')
    pq.write_table(table, './tmp/pyarrow_out/pets1.parquet')
    parquet_file = pq.ParquetFile('./tmp/pyarrow_out/pets1.parquet')
    # print(parquet_file.metadata.row_group(0).column(1).statistics)




