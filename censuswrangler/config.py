"""Module for working with the config file"""

import os

from icecream import ic
import pandas as pd


class Config:
    """Class for working with the config file"""

    def __init__(self, config_path: str):
        # Check if the config file exists then read it into a DataFrame
        self.config_path: str = config_path
        self.config_path_abs: str = os.path.abspath(config_path)
        assert os.path.exists(config_path), f"Config file not found at: {config_path}"
        self.df: pd.DataFrame = pd.read_csv(config_path)

        # Check if the required columns are present
        required_columns = [
            "DATA_FILE_CODE",
            "FIELD_SHORT",
            "FIELD_LONG",
            "VALUE_DESC",
            "GROUP",
        ]
        for column in required_columns:
            assert column in self.df.columns, f"Missing required column: {column}"

        # Cast all columns as strings
        self.df = self.df.astype(str)

        # Count rows
        self.row_count: int = len(self.df)
        self.unique_row_count: int = len(self.df.drop_duplicates())

        # Assert that there are no duplicate rows
        if self.row_count != self.unique_row_count:
            print("Duplicate rows found in config file, these will be ignored")

        # Get unique values for each column
        self.unique_data_file_code: list = self.df["DATA_FILE_CODE"].unique().tolist()
        self.unique_data_file_code_count: int = len(self.unique_data_file_code)

        self.unique_field_short: list = self.df["FIELD_SHORT"].unique().tolist()
        self.unique_field_short_count: int = len(self.unique_field_short)

        self.unique_field_long: list = self.df["FIELD_LONG"].unique().tolist()
        self.unique_field_long_count: int = len(self.unique_field_long)

        self.unique_value_desc: list = self.df["VALUE_DESC"].unique().tolist()
        self.unique_value_desc_count: int = len(self.unique_value_desc)

        self.unique_group: list = self.df["GROUP"].unique().tolist()
        self.unique_group_count: int = len(self.unique_group)

    def summary(self):
        """Prints a summary of the config file"""
        ln = "-" * 50
        print(ln)
        print("Config file summary")
        print(ln)
        ic(self.config_path)
        ic(self.config_path_abs)
        ic(self.row_count)
        ic(self.unique_row_count)
        print(ln)
        ic(self.unique_data_file_code_count)
        ic(self.unique_field_short_count)
        ic(self.unique_field_long_count)
        ic(self.unique_value_desc_count)
        ic(self.unique_group_count)
        print(ln)
        ic(self.unique_data_file_code)
        ic(self.unique_field_short)
        ic(self.unique_field_long)
        ic(self.unique_value_desc)
        ic(self.unique_group)
        print(ln)


if __name__ == "__main__":
    from icecream import ic

    # Example usage with the config template
    config = Config("censuswrangler/config_template.csv")
    config.summary()
