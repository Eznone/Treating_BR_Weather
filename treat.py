import xarray as xr
import pandas as pd  # Import pandas for CSV operations
import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Load the combined_dataset.csv file
csv_file_path = "combined_dataset.csv"
df = pd.read_csv(csv_file_path)

# 3.1 Descriptive Statistics
def descriptive_statistics(data):
    print("Descriptive Statistics:")
    print(data.describe())
    print("\n")

# 3.2 Distribution Visualizations
def distribution_visualizations(data, numeric_columns):
    for col in numeric_columns:
        plt.figure(figsize=(10, 5))
        sns.histplot(data[col], kde=True, bins=30)
        plt.title(f"Distribution of {col}")
        plt.show()

# 3.3 Correlation Analysis
def correlation_analysis(data, numeric_columns):
    correlation_matrix = data[numeric_columns].corr()
    plt.figure(figsize=(12, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Matrix")
    plt.show()

# 3.4 Scatter Plots
def scatter_plots(data, numeric_columns):
    for i, col1 in enumerate(numeric_columns):
        for col2 in numeric_columns[i+1:]:
            plt.figure(figsize=(8, 6))
            sns.scatterplot(x=data[col1], y=data[col2])
            plt.title(f"Scatter Plot: {col1} vs {col2}")
            plt.show()

# 3.5 Categorical Data Analysis
def categorical_data_analysis(data, categorical_columns):
    for col in categorical_columns:
        plt.figure(figsize=(10, 5))
        sns.countplot(x=data[col])
        plt.title(f"Category Distribution: {col}")
        plt.show()

# 3.6 Temporal Analysis
def temporal_analysis(data, time_column, numeric_columns):
    if time_column in data.columns:
        data[time_column] = pd.to_datetime(data[time_column])
        for col in numeric_columns:
            plt.figure(figsize=(12, 6))
            plt.plot(data[time_column], data[col])
            plt.title(f"Temporal Analysis of {col}")
            plt.xlabel("Time")
            plt.ylabel(col)
            plt.show()

# 3.7 Geospatial Analysis
def geospatial_analysis(data, lat_column, lon_column):
    if lat_column in data.columns and lon_column in data.columns:
        plt.figure(figsize=(10, 8))
        plt.scatter(data[lon_column], data[lat_column], alpha=0.5)
        plt.title("Geospatial Distribution")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.show()

# 3.8 Important Insights
def summarize_insights():
    print("Summary of Insights:")
    print("- Observed patterns and trends.")
    print("- Relevant variables identified.")
    print("- Potential challenges and issues noted.")
    print("\n")

# Example usage
if __name__ == "__main__":
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_columns = df.select_dtypes(include=["object", "category"]).columns.tolist()
    
    descriptive_statistics(df)
    distribution_visualizations(df, numeric_columns)
    correlation_analysis(df, numeric_columns)
    scatter_plots(df, numeric_columns)
    categorical_data_analysis(df, categorical_columns)
    # Uncomment and modify the following lines if temporal or geospatial data is present
    # temporal_analysis(df, "time_column_name", numeric_columns)
    # geospatial_analysis(df, "latitude_column_name", "longitude_column_name")
    summarize_insights()


