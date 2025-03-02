import json

import pandas as pd


def pretty_dict(target_dict):
    """Function to pretty print dictionaries, with handling of contained dataframes"""
    # Deals with dataframes in the dict
    def if_df_in_dict(obj):
        if isinstance(obj, pd.DataFrame):
            return "DataFrame Object"
        raise TypeError(f"{type(obj)} not supported")

    print(json.dumps(target_dict, default=if_df_in_dict, indent=4))
