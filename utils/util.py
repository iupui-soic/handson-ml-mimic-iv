"""
This file will contain all the utility class and methods.
"""

import pandas as pd
import dask.dataframe as dd
from pyparsing import col

class generic_utils:
    def __init__(self):
        pass

    def remove_duplicates_and_re_index(self, df, key):
        """
        This method is used to drop duplicate where "key" is column and set that "key" as index.
        """
        df = df.drop_duplicates(subset = key)
        df = df.set_index(key)
        return df

    def replace_nan(self, df, column_name, value_to_replace):
        df.loc[(df[column_name].isna()), column_name] = value_to_replace
        return df

    def replace_column_value(self, df, column_name, threshold, value_to_replace):
        mask = df[column_name].map(df[column_name].value_counts()) < threshold
        df[column_name] = df[column_name].mask(mask, value_to_replace)
        return df