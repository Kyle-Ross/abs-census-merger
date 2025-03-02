import datetime
import os

import pandas as pd

from config import Config
from datapack import Datapack


class Census:
    def __init__(self, census_folder_path, config_path, geo_type):
        """Census class to manage common config and datapack objects, and the methods acting on them"""
        self.census_folder_path = census_folder_path  # Where the census folder is
        self.config_path = config_path  # Where the config file is saved
        self.geo_type = geo_type  # What spatial aggregation sub-folder to target   
        self.config = Config(config_path)
        self.datapack = Datapack(census_folder_path, geo_type, self.config)

    # Function to gather, filter & join specified census files
    def wrangle(
        self,
        output_mode="all",  # Select the output mode, 'merge', 'pivot' or 'all'
        output_folder="",  # Set the location of the output folder, will be the script location by default
        col_desc="short",  # Can be 'short' or 'long'
        col_affix="prefix",  # Affix a 'prefix', 'suffix' or 'none' of the csv's file code to each col
    ):
        # Set the output folder to be a sub-folder of the script folder if unchanged
        if output_folder == "":
            output_folder = os.path.dirname(os.path.abspath(__file__))

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
                if col_desc == "short":
                    new_col_name = old_col_name
                elif col_desc == "long":
                    new_col_name = new_col_name
                else:
                    print(
                        "col_desc must be either 'short or 'long' - incorrect value entered. Reverting to short."
                    )
                    new_col_name = old_col_name

                # Adding a prefix or suffix depending on arguments
                if col_affix == "prefix":
                    new_col_name = (
                        file_details["nameparts"]["file_code"] + "_" + new_col_name
                    )
                elif col_affix == "suffix":
                    new_col_name = (
                        new_col_name + "_" + file_details["nameparts"]["file_code"]
                    )
                elif col_affix == "none":
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
            primary_key_col = str(self.geo_type) + "_CODE_2021"

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

        # Common name element
        current_dt = datetime.datetime.now().strftime("%Y-%m-%d %H-%M")
        file_name_end = (
            "-"
            + self.geo_type
            + "_"
            + col_desc
            + "_"
            + col_affix
            + "-"
            + current_dt
            + ".csv"
        )

        # ------------
        # Merge mode
        # ------------
        # Create an empty dataframe to store the merged data
        if output_mode == "merge" or output_mode == "all":
            merged_df = pd.DataFrame()

            # Get all prepared dataframes in a list
            prepared_dfs = [detail["prepared_df"] for detail in self.datapack.details]

            # Loop through each dataframe in the list and merge with the 'merged_df'
            for df in prepared_dfs:
                if merged_df.empty:
                    merged_df = df
                else:
                    merged_df = pd.merge(
                        merged_df, df, on=primary_key_col, validate="one_to_one"
                    )

            # File name for the merge output type
            merge_output_file_name = "Census Data - Merge" + file_name_end

        # ------------
        # Pivot mode
        # ------------
        if output_mode == "pivot" or output_mode == "all":
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
                new_df = merged_df[group_columns]

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

            # File name for the pivot concat output type
            pivot_concat_output_file_name = "Census Data - Pivot" + file_name_end

        # Conditionally Output the csv
        if output_mode == "merge":
            merged_df.to_csv(
                os.path.join(output_folder, merge_output_file_name), index=False
            )
        elif output_mode == "pivot":
            pivot_concat_df.to_csv(
                os.path.join(output_folder, pivot_concat_output_file_name), index=False
            )
        elif output_mode == "all":
            merged_df.to_csv(
                os.path.join(output_folder, merge_output_file_name), index=False
            )
            pivot_concat_df.to_csv(
                os.path.join(output_folder, pivot_concat_output_file_name), index=False
            )
        else:
            print(
                "output_mode must be 'merge', 'pivot' or 'all' - wrong value entered. Reverting to merge output"
            )
            merged_df.to_csv(
                os.path.join(output_folder, merge_output_file_name), index=False
            )


if __name__ == "__main__":
    # Test code

    census = Census(
        census_folder_path=r"E:/Data/2021_GCP_all_for_AUS_short-header/2021 Census GCP All Geographies for AUS",
        config_path=r"censuswrangler/config_template.csv",
        geo_type="LGA",
    )

    # Calling the function
    census.wrangle(
        output_mode="all",
        col_desc="long",
        col_affix="prefix",
    )
