"""Functions for working with the census datapack folder"""

import os

from icecream import ic

from config import Config


class Datapack:
    """Class for working with selections of the census datapack folder"""

    def __init__(self, folder_path: str, geo_type: str, config: Config):
        self.folder_path = folder_path

        # Build a dictionary containing information of the files in the census datapack folder
        datapack_details = []
        for root, directories, files in os.walk(folder_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                # Split the filename into its name and type components
                name, file_type = os.path.splitext(filename)
                # Split the name by '_'
                name_parts = name.split("_")
                # Split the path into its components
                path_parts = os.path.split(file_path)
                # Create a dictionary with the path components as key-value pairs
                file_dict = {
                    "filename": name,
                    "nameparts": {
                        "census_desc": name_parts[0],
                        "file_code": name_parts[1],
                        "country": name_parts[2],
                        "geo_type": name_parts[3],
                    },
                    "filetype": file_type,
                    "directory": path_parts[0],
                    "full_path": file_path,
                }
                # Add the dictionary to the list
                datapack_details.append(file_dict)
                # Filter the list to only include the target geo_type and data file codes
                datapack_details = [
                    file_info_dict
                    for file_info_dict in datapack_details
                    if (
                        file_info_dict["nameparts"]["geo_type"] == geo_type
                        and file_info_dict["nameparts"]["file_code"]
                        in config.unique_data_file_code
                        and file_info_dict["filetype"] == ".csv"
                    )
                ]
        self.details = datapack_details

    def summary(self):
        """Prints a summary of the datapack selection"""
        ic(datapack.details)


if __name__ == "__main__":
    folder_path = r"E:/Data/2021_GCP_all_for_AUS_short-header/2021 Census GCP All Geographies for AUS"
    config = Config("censuswrangler/config_template.csv")
    datapack = Datapack(folder_path, "LGA", config)
    datapack.summary()
