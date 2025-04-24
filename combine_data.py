import xarray as xr
import pandas as pd  # Import pandas for CSV operations
import os
import numpy as np

# define per‐state bounds
state_bounds = {
    "Amazonas":  {"latbounds": [-8.5, 2.5],  "lonbounds": [-74, -60]},
    "Pará":      {"latbounds": [-8,   1],    "lonbounds": [-60, -48]},
    "Acre":      {"latbounds": [-11, -7],    "lonbounds": [-74, -67]},
    "Rondônia":  {"latbounds": [-13.5,-8.5], "lonbounds": [-67, -60]},
    "Roraima":   {"latbounds": [1,    5],    "lonbounds": [-64, -60]},
    "Amapá":     {"latbounds": [-1,   4],    "lonbounds": [-54, -51]},
    "Mato Grosso":{"latbounds": [-12.5,-8],  "lonbounds": [-60, -52]},
    "Tocantins": {"latbounds": [-9,   -5],   "lonbounds": [-50, -47]},
    "Maranhão":  {"latbounds": [-7,   -2],   "lonbounds": [-48, -45]}
}

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

# Process files for a specific year and state
def process_year(state, year, file_paths, bounds, filtered_folder):
    print(f"Processing {state} for year {year}")
    combined_data = None

    for fp in file_paths:
        ds = xr.open_dataset(fp)
        sub = subset_dataset(ds, bounds["latbounds"], bounds["lonbounds"])
        if combined_data is None:
            combined_data = sub
        else:
            combined_data = xr.concat([combined_data, sub], dim='time')

    df = combined_data.to_dataframe().reset_index()
    # average over the entire state area per time
    df = df.groupby(['time']).mean().reset_index()
    df.rename(columns={'time': 'date'}, inplace=True)
    df['state'] = state

    df.rename(columns={
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

    save_to_csv(df, state, year, filtered_folder)


# Save the processed data to a CSV file
def save_to_csv(df, state, year, filtered_folder):
    csv_path = os.path.join(filtered_folder, f"{state}_{year}.csv")
    df.to_csv(csv_path, index=False, header=True)
    print(f"{state} {year} → {csv_path}")

# Main function
def main():
    unfiltered, filtered, grouped = setup_directories()
    files_by_year = group_files_by_year(unfiltered)

    for state, bounds in state_bounds.items():
        for year, fps in files_by_year.items():
            process_year(state, year, fps, bounds, filtered)

    files_by_year = group_files_by_year(filtered)
    group_years(files_by_year, grouped)

if __name__ == "__main__":
    main()