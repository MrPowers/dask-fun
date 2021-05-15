import pytest
import pandas as pd
from dask import dataframe as dd


def describe_add():
    def it_adds_to_all_numeric_columns():
        pdf = pd.DataFrame(
            {"angles": [1, 2, 3], "degrees": [90, 180, 270]},
            index=["circle", "triangle", "rectangle"],
        )
        df = dd.from_pandas(pdf, npartitions=2)
        df = df + 1
        expected = pd.DataFrame(
            {"angles": [2, 3, 4], "degrees": [91, 181, 271]},
            index=["circle", "triangle", "rectangle"],
        )
        pd.testing.assert_frame_equal(df.compute(), expected, check_like=True)

    def it_adds_column_to_pandas_df():
        df = pd.DataFrame(
            {"num1": [1, 2, 3], "num2": [2, 2, 5]},
        )
        df["num3"] = df["num1"] + df["num2"]
        expected = pd.DataFrame(
            {"num1": [1, 2, 3], "num2": [2, 2, 5], "num3": [3, 4, 8]},
        )
        pd.testing.assert_frame_equal(df, expected)

    def it_adds_to_a_specific_column():
        pdf = pd.DataFrame(
            {"num1": [1, 2, 3], "num2": [2, 2, 5]},
        )
        df = dd.from_pandas(pdf, npartitions=2)
        df["num3"] = df["num1"] + df["num2"]
        expected = pd.DataFrame(
            {"num1": [1, 2, 3], "num2": [2, 2, 5], "num3": [3, 4, 8]},
        )
        pd.testing.assert_frame_equal(df.compute(), expected)
