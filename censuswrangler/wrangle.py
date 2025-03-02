from typing import Dict, Optional
import copy
import datetime
import os

import pandas as pd

from config import Config
from datapack import Datapack


class Census:
    """Census class to encapsulate common config and datapack objects, and the methods acting on them"""

    def __init__(
        self,
        census_folder_path: str,
        config_path: str,
        geo_type: str,
        year: int,
        col_type: str = "short",
        affix_type: str = "prefix",
    ):
        self.census_folder_path: str = census_folder_path  # Where the census folder is
        self.config_path: str = config_path  # Where the config file is saved
        self.geo_type: str = geo_type  # What spatial aggregation sub-folder to target
        self.year: int = year  # Helps find columns in the datapack, which have the census year as a suffix
        self.col_type: str = col_type  # Can be 'short' or 'long'
        self.affix_type: str = affix_type  # Affix a 'prefix', 'suffix' or 'none' of the csv's file code to each col, and put arg on file name
        self.config: Config = Config(config_path)
        self.datapack: Datapack = Datapack(census_folder_path, geo_type, self.config)
        self._allowed_output_modes: Dict[Dict] = {
            "merge": {
                "requirement": "First run the Census.wrangle method with mode = 'merge'"
            },
            "pivot": {
                "requirement": "First run the Census.wrangle method with mode = 'pivot'"
            },
            "all": {
                "requirement": "First run the Census.wrangle method with mode = 'all'"
            },
        }

        # Assertions
        allowed_col_types = ("short", "long")
        assert col_type in allowed_col_types, (
            f"col_type argument '{col_type} not in allowed types {allowed_col_types}"
        )

        allowed_affix_types = ("prefix", "suffix", "none")
        assert affix_type in allowed_affix_types, (
            f"affix_type argument '{affix_type} not in allowed types {allowed_affix_types}"
        )

        # Dataframes to store the merged and pivoted data (once wrangle is called)
        self.merged_df: Optional[pd.DataFrame] = None
        self.pivoted_df: Optional[pd.DataFrame] = None

    def _assert_mode_arg(self, mode):
        """Internal function to check the mode is in the allowed values"""
        allowed = self._allowed_output_modes.keys()
        assert mode in allowed, (
            f"mode argument '{mode}' not in allowed modes '{allowed}'"
        )

    def wrangle(self, mode):
        """Function to gather, filter & join specified census files using the config and datapack objects in the census class"""

        self._assert_mode_arg(mode)

        # ===========
        # Prepare target dataframes
        # ===========

        # List to store column, name
        col_details = []

        # Looping through the per-file-code dictionaries, reading and filtering the resulting dataframes per the config
        for file_details in self.datapack.details:
            # Prepare the dataframe
            file_path = file_details["full_path"]
            unfiltered_df = pd.read_csv(file_path)
            file_details["unfiltered_df"] = unfiltered_df

            # Grab the current file code
            file_code = file_details["nameparts"]["file_code"]

            # Get the config, and select the rows that match the current file code
            # Save the df as a list of lists, where each list the values in the row
            df = self.config.df
            df = df[df["DATA_FILE_CODE"] == file_code]
            df = df.drop(columns=["DATA_FILE_CODE"])
            config_rows = df.values.tolist()

            # Dictorary to store the old and new column names before renaming
            col_name_dict = {}

            # Looping over the list of config rows
            # Prepares a dictionary mainly used to create new column names depending on conditions
            for row in config_rows:
                # Getting variables from list
                old_col_name = row[0]  # FIELD_SHORT
                new_col_name = row[1]  # FIELD_LONG
                value_desc = row[2]  # VALUE_DESC
                col_group = row[3]  # GROUP

                # Setting the replacement column name conditionally depending on arguments
                if self.col_type == "short":
                    new_col_name = old_col_name
                elif self.col_type == "long":
                    new_col_name = new_col_name
                else:
                    print(
                        "col_desc must be either 'short or 'long' - incorrect value entered. Reverting to short."
                    )
                    new_col_name = old_col_name

                # Adding a prefix or suffix depending on arguments
                if self.affix_type == "prefix":
                    new_col_name = (
                        file_details["nameparts"]["file_code"] + "_" + new_col_name
                    )
                elif self.affix_type == "suffix":
                    new_col_name = (
                        new_col_name + "_" + file_details["nameparts"]["file_code"]
                    )
                elif self.affix_type == "none":
                    # Leave var unchanged
                    new_col_name = new_col_name
                else:
                    print(
                        "col_desc must be 'prefix', 'suffix' or 'none' - incorrect value entered. Reverting to none."
                    )

                # Adding the old and new key combination to the outer dictionary
                col_name_dict[old_col_name] = new_col_name

                # Adding all column group dictionary to the associated list
                # Creating the dictionary
                col_detail = {
                    "old_col": old_col_name,
                    "new_col": new_col_name,
                    "group": col_group,
                    "value_desc": value_desc,
                }

                # Appending that to the list
                col_details.append(col_detail)

            # Getting a list with just the old col names (which are the keys)
            old_col_list = list(col_name_dict.keys())

            # Appending the target columns to the dictionary
            file_details["target_columns"] = col_name_dict

            # Establishing the name of the primary key column
            # This is basically the geocode with a suffix
            primary_key_col = f"{self.geo_type}_CODE_{self.year}"

            # Adding that to the list of old columns which is used to filter below
            old_col_list.insert(0, primary_key_col)

            # Renaming and filtering columns using the config data
            prepared_df = unfiltered_df.loc[:, old_col_list].rename(
                columns=col_name_dict
            )

            # Saving the prepared_df df to the file_details dict, which is in turn saved inplace to datapack.details
            file_details["prepared_df"] = prepared_df

        # ===========
        # Preparing outputs
        # ===========

        # ------------
        # Merge mode
        # ------------
        # Create an empty dataframe to store the merged data
        # Used in the pivot mode as well
        if mode == "merge" or mode == "pivot" or mode == "all":
            # Get all prepared dataframes in a list
            prepared_dfs = [detail["prepared_df"] for detail in self.datapack.details]

            # Loop through each dataframe in the list and merge with the 'merged_df'
            # Use the first df as the base and merge the rest on the primary key column
            for df in prepared_dfs:
                if self.merged_df is None:
                    self.merged_df = df
                else:
                    self.merged_df = pd.merge(
                        self.merged_df, df, on=primary_key_col, validate="one_to_one"
                    )

        # ------------
        # Pivot mode
        # ------------
        if mode == "pivot" or mode == "all":
            # Reworking the dictionary containing group and column information
            # Defining the new structure as a dict of lists like {'group': ['col1', 'col2', 'col3'],...}
            group_dict = {}

            for col_detail in col_details:
                group_key = col_detail["group"]
                new_col_value = col_detail["new_col"]
                if group_key not in group_dict:
                    group_dict[group_key] = []
                if new_col_value not in group_dict[group_key]:
                    group_dict[group_key].append(new_col_value)

            # Defining a list to contain output dataframes, which will be used to concat
            pivoted_dfs_list = []

            # Looping over the dictionary to subset, unpivot and create the new 'pivot' dataframes
            for (
                key_group,
                value_col_list,
            ) in (
                group_dict.copy().items()
            ):  # To avoid runtime errors to adding to a dict which being looped over
                # Creating a new list that includes the id column
                group_columns = value_col_list
                group_columns.append(primary_key_col)

                # Create a subset of the merged dataframe containing only columns from the group
                new_df = copy.deepcopy(self.merged_df[group_columns])

                # Creating a basic dictionary with the old (key) and new names (value)
                value_desc_dict = {}

                for ref_dict in col_details:
                    value_desc_dict[f"{ref_dict['new_col']}"] = ref_dict["value_desc"]

                # Using that dictionary to rename columns
                new_df = new_df.rename(columns=value_desc_dict)

                # Getting all columns that are not the primary key column for the pivoting function
                cols_to_unpivot = new_df.columns.difference([primary_key_col])

                # Unpivot dataframe
                new_df_unpivoted = new_df.melt(
                    id_vars=[primary_key_col],
                    value_vars=cols_to_unpivot,
                    var_name=key_group,
                    value_name=f"{key_group} Value",
                )

                # Appending those dataframes to the results list
                pivoted_dfs_list.append(new_df_unpivoted)

            # Concat-ing all unpivoted dfs
            pivot_concat_df = pd.concat(pivoted_dfs_list)

            # Assign it to the class attribute
            self.pivoted_df = pivot_concat_df

    def to_csv(self, mode: str, output_folder: str):
        """Function to output the csv files to a output folder"""

        self._assert_mode_arg(mode)

        # Check the folder directory
        assert os.path.isdir(output_folder), (
            f"The path '{output_folder}' is not a directory or does not exist."
        )

        # Common file name elements
        current_dt = datetime.datetime.now().strftime("%Y-%m-%d %H-%M")
        file_name_end = (
            "-"
            + self.geo_type
            + "_"
            + self.col_type
            + "_"
            + self.affix_type
            + "-"
            + current_dt
            + ".csv"
        )

        # Set names for the output types
        merge_name = "Census Data - Merge" + file_name_end
        pivot_name = "Census Data - Pivot" + file_name_end

        # Common csv output function
        def df_to_csv(df, name, index=False):
            df.to_csv(os.path.join(output_folder, name), index=index)

        # Conditionally Output the csv
        if mode == "merge":
            df_to_csv(self.merged_df, merge_name)
        elif mode == "pivot":
            df_to_csv(self.pivoted_df, pivot_name)
        elif mode == "all":
            df_to_csv(self.merged_df, merge_name)
            df_to_csv(self.pivoted_df, pivot_name)
        else:
            raise ValueError(
                f"'{mode}' is invalid. mode must be one of allowed values {self._allowed_output_modes.keys()}"
            )


if __name__ == "__main__":
    from icecream import ic

    census = Census(
        census_folder_path=r"E:/Data/2021_GCP_all_for_AUS_short-header/2021 Census GCP All Geographies for AUS",
        config_path=r"censuswrangler/config_template.csv",
        geo_type="LGA",
        year=2021,
    )

    census.wrangle("all")
    ic(census.merged_df)
    census.to_csv("all", r"F:/Github/censuswrangler/test_output")