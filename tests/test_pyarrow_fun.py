import pytest

import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow

import os

os.makedirs("./tmp/pyarrow_out", exist_ok=True)


def test_pyarrow_compression():
    table = pv.read_csv("./data/people/people1.csv")
    pq.write_table(table, "./tmp/pyarrow_out/people1.parquet")
    parquet_file = pq.ParquetFile("./tmp/pyarrow_out/people1.parquet")
    # print(parquet_file.metadata)
    # print(parquet_file.metadata.row_group(0))
    # print(parquet_file.metadata.row_group(0).column(0))
    # print(parquet_file.metadata.row_group(0).column(0).statistics)
    assert parquet_file.metadata.row_group(0).column(0).compression == "SNAPPY"
    # parquet_file = pq.ParquetFile('example.parquet')
    # parquet_file = pq.ParquetFile('./tmp/people_parquet2/part.0.parquet')
    # parquet_file = pq.ParquetFile('./tmp/people_compressed_parquet/part.0.parquet')
    # parquet_file.metadata
    # parquet_file.schema


def test_pyarrow_statistics():
    table = pv.read_csv("./data/pets/pets1.csv")
    pq.write_table(table, "./tmp/pyarrow_out/pets1.parquet")
    parquet_file = pq.ParquetFile("./tmp/pyarrow_out/pets1.parquet")
    stats = parquet_file.metadata.row_group(0).column(1).statistics
    assert stats.min == 1
    assert stats.max == 9


def test_add_custom_metadata():
    table = pv.read_csv("./data/pets/pets1.csv")
    # print('')
    # print(table.schema.metadata)
    # s2 = table.schema.with_metadata({b'say_hi': b'hola'})
    # print(s2.metadata)
    custom_metadata = {"sample_number": "12", "date_obtained": "Tuesday"}
    existing_metadata = table.schema.metadata
    merged_metadata = {**custom_metadata, **(existing_metadata or {})}
    fixed_table = table.replace_schema_metadata(merged_metadata)
    pyarrow.parquet.write_table(
        fixed_table, "./tmp/pyarrow_out/pets_with_metadata.parquet"
    )
    parquet_table = pq.read_table("./tmp/pyarrow_out/pets_with_metadata.parquet")
    print(parquet_table.schema.metadata[b"sample_number"])
    # parquet_file = pq.ParquetFile('./tmp/pyarrow_out/pets_with_metadata.parquet')
    # print(parquet_file.schema_arrow)
    # print(parquet_file.schema.metadata)
