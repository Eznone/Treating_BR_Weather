import xarray as xr
import pandas as pd  # Import pandas for CSV operations
import os
import numpy as np

# Ensure the 'data/Unfiltered' and 'data/Filtered' folders exist
def setup_directories():
    unfiltered_folder = os.path.join(os.path.dirname(__file__), "data", "Unfiltered")
    filtered_folder = os.path.join(os.path.dirname(__file__), "data", "Filtered")
    os.makedirs(unfiltered_folder, exist_ok=True)
    os.makedirs(filtered_folder, exist_ok=True)
    return unfiltered_folder, filtered_folder

# Group files by year
def group_files_by_year(unfiltered_folder):
    files_by_year = {}
    for dataset_name in os.listdir(unfiltered_folder):
        if dataset_name.endswith(".nc"):
            year = dataset_name.split("_")[-1].replace(".nc", "")  # Extract the year from the filename
            if year not in files_by_year:
                files_by_year[year] = []
            files_by_year[year].append(os.path.join(unfiltered_folder, dataset_name))
    return files_by_year

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

    # Average data for duplicate dates
    combined_df = combined_data.to_dataframe().reset_index()
    combined_df = combined_df.groupby("time").mean().reset_index()  # Average data by date

    # Save the processed data to a CSV file
    save_to_csv(combined_df, year, filtered_folder)

# Save the processed data to a CSV file
def save_to_csv(combined_df, year, filtered_folder):
    csv_path = os.path.join(filtered_folder, f"{year}.csv")
    write_header = True  # Flag to write the header only once
    batch_size = 3  # Number of rows to process in each batch
    batch_data = []  # Temporary storage for batch data

    for i, row in combined_df.iterrows():
        batch_data.append(row.to_frame().T)  # Add the row to the batch

        # Write the batch to the CSV file when the batch is full
        if (i + 1) % batch_size == 0 or i == len(combined_df) - 1:
            print(f"Writing batch to CSV (rows {i - batch_size + 2} to {i + 1})...")
            pd.concat(batch_data).to_csv(csv_path, mode="a", index=False, header=write_header)
            write_header = False  # Disable header writing after the first batch
            batch_data = []  # Clear the batch

    print(f"Dataset for year {year} saved to {csv_path}")

# Main function
def main():
    latbounds = [-9, 1.45]  # Latitude range
    lonbounds = [-73, -58]  # Longitude range

    unfiltered_folder, filtered_folder = setup_directories()
    files_by_year = group_files_by_year(unfiltered_folder)

    for year, file_paths in files_by_year.items():
        process_year(year, file_paths, latbounds, lonbounds, filtered_folder)

if __name__ == "__main__":
    main()