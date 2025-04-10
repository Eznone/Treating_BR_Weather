import xarray as xr
import pandas as pd  # Import pandas for CSV operations
import os
import requests  # Import requests to download the dataset
import numpy as np
from datetime import date

# Ensure the 'data/Unfiltered' and 'data/Filtered' folders exist
unfiltered_folder = os.path.join(os.path.dirname(__file__), "data", "Unfiltered")
filtered_folder = os.path.join(os.path.dirname(__file__), "data", "Filtered")
os.makedirs(unfiltered_folder, exist_ok=True)
os.makedirs(filtered_folder, exist_ok=True)

# Define bounding lat/lon for the Amazon Rainforest
latbounds = [-9, 1.45]  # Latitude range
lonbounds = [-73, -58]  # Longitude range

# Iterate over each file in the 'unfiltered_folder'
for dataset_name in os.listdir(unfiltered_folder):
    local_file_path = os.path.join(unfiltered_folder, dataset_name)

    # Open the dataset
    print(f"Opening dataset: {dataset_name}")
    dataset = xr.open_dataset(local_file_path)

    # Subset in space (lat/lon)
    lat = dataset['lat'].values
    lon = dataset['lon'].values
    lat_index_min = (np.abs(lat - latbounds[0])).argmin()
    lat_index_max = (np.abs(lat - latbounds[1])).argmin()
    lon_index_min = (np.abs(lon - lonbounds[0])).argmin()
    lon_index_max = (np.abs(lon - lonbounds[1])).argmin()

    lat_index_range = slice(min(lat_index_min, lat_index_max), max(lat_index_min, lat_index_max) + 1)
    lon_index_range = slice(min(lon_index_min, lon_index_max), max(lon_index_min, lon_index_max) + 1)

    # Subset the dataset
    subset = dataset.isel(lat=lat_index_range, lon=lon_index_range)

    # Initialize the CSV file and set a flag for writing the header
    csv_path = os.path.join(filtered_folder, f"{dataset_name.replace('.nc', '.csv')}")
    write_header = True  # Flag to write the header only once

    # Process the subset instead of the full dataset
    batch_size = 3  # Number of time steps to process in each batch
    batch_data = []  # Temporary storage for batch data

    for i, time_step in enumerate(subset['time']):
        print(f"Processing time step: {time_step.values}")
        chunk = subset.sel(time=time_step)  # Select data for the current time step
        chunk_df = chunk.to_dataframe().reset_index()  # Convert the chunk to a DataFrame
        batch_data.append(chunk_df)  # Add the chunk to the batch

        # Write the batch to the CSV file when the batch is full
        if (i + 1) % batch_size == 0 or i == len(subset['time']) - 1:
            print(f"Writing batch to CSV (time steps {i - batch_size + 2} to {i + 1})...")
            pd.concat(batch_data).to_csv(csv_path, mode="a", index=False, header=write_header)
            write_header = False  # Disable header writing after the first batch
            batch_data = []  # Clear the batch

    print(f"Dataset saved to {csv_path}")