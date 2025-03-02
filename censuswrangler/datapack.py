"""Functions for working with the census datapack folder"""

import os


def info(folder_path):
    """Build a dictionary containing information of the files in the census datapack folder"""
    # Get all file paths in the target folder
    file_paths = []
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
            file_paths.append(file_dict)

    # Returning the result
    return file_paths
