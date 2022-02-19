"""
This file will contain all the utility class and methods.
"""

import pandas as pd

class generic_utils:
    def __init__(self):
        pass

    def get_df_shape(self, df_generator):
        """
        This method is used to get the shape of whole dataframe from its chunks.
        """
        row = 0
        col = None
        for item in df_generator:
            col = item.shape[1]
            row += item.shape[0]

        return (row, col)

    def get_full_df(self, df_generator):
        full_df = pd.concat([df_chunk for df_chunk in df_generator])
        return full_df

    def print_df_head(self, df_generator):
        for item in df_generator:
            display(item.head())
            break
