import xarray as xr
import pandas as pd  # Import pandas for CSV operations
import os
import numpy as np

# Ensure the 'data/Unfiltered' and 'data/Filtered' folders exist
def setup_directories():
    unfiltered_folder = os.path.join(os.path.dirname(__file__), "data", "Unfiltered")
    filtered_folder = os.path.join(os.path.dirname(__file__), "data", "Filtered")
    grouped_folder = os.path.join(os.path.dirname(__file__), "data", "Grouped")
    os.makedirs(unfiltered_folder, exist_ok=True)
    os.makedirs(filtered_folder, exist_ok=True)
    os.makedirs(grouped_folder, exist_ok=True)
    return unfiltered_folder, filtered_folder, grouped_folder

# Group files by year
def group_files_by_year(folder):
    files_by_year = {}
    for dataset_name in os.listdir(folder):
        if dataset_name.endswith(".nc") or dataset_name.endswith(".csv"):
            # Determine the file extension and extract the year accordingly
            if dataset_name.endswith(".nc"):
                year = dataset_name.split("_")[-1].replace(".nc", "")  # Extract year from .nc file
            elif dataset_name.endswith(".csv"):
                year = dataset_name.split("_")[-1].replace(".csv", "")  # Extract year from .csv file

            # Group files by year
            if year not in files_by_year:
                files_by_year[year] = []
            files_by_year[year].append(os.path.join(folder, dataset_name))
    return files_by_year

# Grouping all files into one dataset and saving as a CSV file
def group_years(files_by_year, grouped_folder):
    combined_df = None  # Placeholder for the combined DataFrame

    for year, file_paths in files_by_year.items():
        for file_path in file_paths:
            dataset_name = os.path.basename(file_path)
            print(f"Opening dataset: {dataset_name}")
            df = pd.read_csv(file_path)  # Read the CSV file into a DataFrame

            # Ensure labels are not included multiple times
            if combined_df is None:
                combined_df = df  # First dataset, include all rows
            else:
                combined_df = pd.concat([combined_df, df], ignore_index=True)  # Skip the first row of subsequent datasets

    # Save the combined DataFrame to a CSV file
    grouped_file_path = os.path.join(grouped_folder, "combined_dataset.csv")
    print(f"Saving combined dataset to {grouped_file_path}...")
    combined_df.to_csv(grouped_file_path, index=False)
    print("Combined dataset saved successfully.")

# Subset the dataset by latitude and longitude
def subset_dataset(dataset, latbounds, lonbounds):
    lat = dataset['lat'].values
    lon = dataset['lon'].values
    lat_index_min = (np.abs(lat - latbounds[0])).argmin()
    lat_index_max = (np.abs(lat - latbounds[1])).argmin()
    lon_index_min = (np.abs(lon - lonbounds[0])).argmin()
    lon_index_max = (np.abs(lon - lonbounds[1])).argmin()

    lat_index_range = slice(min(lat_index_min, lat_index_max), max(lat_index_min, lat_index_max) + 1)
    lon_index_range = slice(min(lon_index_min, lon_index_max), max(lon_index_min, lon_index_max) + 1)

    return dataset.isel(lat=lat_index_range, lon=lon_index_range)

# Process files for a specific year
def process_year(year, file_paths, latbounds, lonbounds, filtered_folder):
    print(f"Processing files for year: {year}")
    combined_data = None

    for file_path in file_paths:
        dataset_name = os.path.basename(file_path)
        print(f"Opening dataset: {dataset_name}")
        dataset = xr.open_dataset(file_path)

        # Subset the dataset
        subset = subset_dataset(dataset, latbounds, lonbounds)

        # Combine data for the year
        if combined_data is None:
            combined_data = subset
        else:
            combined_data = xr.merge([combined_data, subset])  # Merge datasets

    # Convert the combined dataset to a DataFrame
    combined_df = combined_data.to_dataframe().reset_index()

    # Bin latitude and longitude into groups of 10 and calculate the midpoint
    combined_df['lat_bin'] = (combined_df['lat'] // 10) * 10 + 5  # Bin latitudes and represent as midpoint
    combined_df['lon_bin'] = (combined_df['lon'] // 10) * 10 + 5  # Bin longitudes and represent as midpoint

    # Group by time, lat_bin, and lon_bin, and calculate the mean for other columns
    combined_df = combined_df.groupby(['time', 'lat_bin', 'lon_bin']).mean().reset_index()

    # Rename columns to clearer names
    combined_df.rename(columns={
        'time': 'date',
        'lat_bin': 'latitude_bin',
        'lon_bin': 'longitude_bin',
        'lat': 'average_latitude',
        'lon': 'average_longitude',
        'crs': 'coordinate_reference_system',
        'def': 'deficit',
        'PDSI': 'palmer_drought_severity_index',
        'pet': 'potential_evapotranspiration',
        'ppt': 'precipitation',
        'tmax': 'maximum_temperature',
        'tmin': 'minimum_temperature',
        'vap': 'vapor_pressure',
        'vpd': 'vapor_pressure_deficit',
        'ws': 'wind_speed'
    }, inplace=True)

    # Save the processed data to a CSV file
    save_to_csv(combined_df, year, filtered_folder)

# Save the processed data to a CSV file
def save_to_csv(combined_df, year, filtered_folder):
    csv_path = os.path.join(filtered_folder, f"{year}.csv")
    write_header = True  # Flag to write the header only once

    # Write the entire DataFrame to the CSV file in one go
    combined_df.to_csv(csv_path, index=False, header=write_header)
    print(f"Dataset for year {year} saved to {csv_path}")

# Main function
def main():
    # Adjusted latitude and longitude bounds for the Amazonas state in Brazil
    latbounds = [-10, 2]  # Latitude range (approximately 10°S to 2°N)
    lonbounds = [-74, -56]  # Longitude range (approximately 75°W to 56°W)

    unfiltered_folder, filtered_folder, grouped_folder = setup_directories()
    files_by_year = group_files_by_year(unfiltered_folder)

    for year, file_paths in files_by_year.items():
        process_year(year, file_paths, latbounds, lonbounds, filtered_folder)

    files_by_year = group_files_by_year(filtered_folder)
    group_years(files_by_year, grouped_folder)

if __name__ == "__main__":
    main()