print("Began program")
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#from statsmodels.tsa.stattools import acf,pacf
#from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import os  # Add import for handling file paths

print("Finished importing")

def filter_data_by_year(df):
    """
    Filters the DataFrame to include only rows where the year in the 'Data' column is between 2018 and 2023 (inclusive),
    splits the 'Data' column into 'Year' and 'Month' columns, and ensures a maximum of 5 rows per year and month.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame with a 'Data' column.
    
    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    # Ensure the first column is split into multiple columns if needed
    if len(df.columns) == 1:
        df = df[df.columns[0]].str.split(',', expand=True)  # Split the single column into multiple columns
        df.columns = df.iloc[0].str.strip().str.replace('"', '')  # Use the first row as column names
        df = df[1:]  # Drop the first row as it is now the header
        print("Adjusted DataFrame columns:", df.columns)
    
    # Identify the correct column for dates (e.g., the second column after splitting)
    date_column = df.columns[1]  # Assuming the second column contains the date
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')  # Convert to datetime, handle errors
    df['Year'] = df[date_column].dt.year
    df['Month'] = df[date_column].dt.month
    print("Began filtering")
    
    # Filter rows for the year range
    filtered_df = df[(df['Year'] >= 2018) & (df['Year'] <= 2023)]
    
    # Limit to a maximum of 5 rows per year and month
    filtered_df = (
        filtered_df.groupby(['Year', 'Month'])
        .head(5)
        .reset_index(drop=True)
    )
    print("Finished filtering")
    return filtered_df

def save_filtered_data(df, filename):
    """
    Saves the filtered DataFrame to the 'data/correct_data' folder.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to save.
        filename (str): The name of the file to save the DataFrame as.
    """
    output_dir = 'data/correct_data'
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    output_path = os.path.join(output_dir, filename)
    df.to_csv(output_path, index=False)
    print(f"Filtered data saved to {output_path}")

def list_csv_files_in_archive():
    """
    Lists the names of all CSV files in the 'archive' folder.
    
    Returns:
        list: A list of CSV file names in the 'archive' folder.
    """

    print("Entered csv folder")

    archive_dir = 'data/archive'
    if not os.path.exists(archive_dir):
        print(f"The folder '{archive_dir}' does not exist.")
        return []
    return [f for f in os.listdir(archive_dir) if f.endswith('.csv')]

# Example usage:
# df = pd.read_csv('your_file.csv')
# filtered_df = filter_data_by_year(df)
# save_filtered_data(filtered_df, 'filtered_data.csv')


print("About to enter")
csv_files = list_csv_files_in_archive()
print(csv_files)

# for file in csv_files:
#     file_path = os.path.join('archive', file)
#     df = pd.read_csv(file_path, sep=';', encoding='latin1')
#     filtered_df = filter_data_by_year(df)
#     save_filtered_data(filtered_df, file)

df = pd.read_csv('data/archive/north.csv', sep=';', encoding='latin1')
filtered_df = filter_data_by_year(df)
save_filtered_data(filtered_df, 'filtered_north.csv')