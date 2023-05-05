# abs-census-merger
A Python script which takes the census data pack from the ABS and instantly merges data across the many separate files. Allows you to specify files, fields and spatial aggregation against customizable settings in a config csv file.

## How to use

1. Download your desired [datapack](https://www.abs.gov.au/census/find-census-data/datapacks) on the ABS site
2. Clone this repo
3. Adjust desired files and fields in the config file
4. Open Main.py in your desired IDE
5. Set the path of the downloaded and extracted census folder at the top of the script with the `census_folder_path` variable. This must be the sub-folder containing the data - for example with the 2021 census this is the `'2021 Census GCP All Geographies for AUS'` folder
6. Adjust the function parameters as needed at the bottom of the script with the pre-written function call
7. Run the script

```Python
def accumulate_census(target_folder_path,  # Where the census folder is
                      config_path,  # Where the config folder is saved
                      geo_type,  # What spatial aggregation sub-folder to target
                      output_folder='',  # Set the location of the output folder, will be the script location by default
                      col_desc='short',  # Can be 'short' or 'long'
                      col_affix='prefix'):  # Affix a 'prefix', 'suffix' or 'none' of the csv's file code to each col
```

## Notes

- Example output is included
- Geo types include LGA, POA, SA1, SA2 etc
- col_desc will determine whether column names are updated to unabbreviated versions via the config file
- Use the metadata .xlsx file included in the datapack download to set up the config file with the fields you want