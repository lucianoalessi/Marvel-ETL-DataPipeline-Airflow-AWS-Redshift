import requests
import pandas as pd
import numpy as np
from datetime import datetime
import os

def get_data(base_url, endpoint, params=None):
    """
    Performs a paginated GET request to an API to retrieve all available data.

    Parameters:
    base_url (str): The base URL of the API.
    endpoint (str): The specific endpoint of the API to make the request to.
    params (dict, optional): The request parameters, including pagination (limit and offset).

    Returns:
    list: A list with all the results retrieved from the API.
    None: If an error occurs during the request.
    """
    try:
        all_results = []
        limit = 100  # Maximum number of results per page allowed by the API
        offset = 0   # Pagination starts from the first result
        
        while True:
            # Update the 'offset' and 'limit' parameters in each request
            params.update({'limit': limit, 'offset': offset})
            response = requests.get(f"{base_url}/{endpoint}", params=params)
            response.raise_for_status()
            
            data = response.json()
            results = data['data']['results']
            all_results.extend(results)  # Store the retrieved results
            
            # Check if there are more results to fetch
            total_results = data['data']['total']
            if len(all_results) >= total_results:
                break
            
            # Update the offset to retrieve the next page of results
            offset += limit
        
        return all_results
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

def get_comics(base_url, endpoint, params=None):
    """
    Performs a paginated GET request to the API to retrieve comic data, limiting the total to 500 results.
    This limit of 500 is set to simplify the practice and testing of the data pipeline.

    Parameters:
    base_url (str): The base URL of the API.
    endpoint (str): The specific endpoint of the API for comics.
    params (dict, optional): The request parameters, including pagination (limit and offset).

    Returns:
    list: A list with the results retrieved from the API, limited to 1000 items.
    None: If an error occurs during the request.
    """
    try:
        all_results = []
        limit = 100  # Maximum number of results per page allowed by the API
        offset = 0   # Pagination starts from the first result
        
        while True:
            # Update the 'offset' and 'limit' parameters in each request
            params.update({'limit': limit, 'offset': offset})
            response = requests.get(f"{base_url}/{endpoint}", params=params)
            response.raise_for_status()
            
            data = response.json()
            results = data['data']['results']
            all_results.extend(results)  # Store the retrieved results
            
            # Check if there are more results to fetch
            total_results = data['data']['total']
            #if len(all_results) >= total_results:
            if len(all_results) >= 500:
                break
            
            # Update the offset to retrieve the next page of results
            offset += limit
        
        return all_results
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    
def build_table(json_data, record_path=None):
    """
    Builds a pandas DataFrame from JSON formatted data.

    Parameters:
    json_data (dict/list): The data in JSON format obtained from an API.

    Returns:
    DataFrame: A pandas DataFrame containing the data, or None if there's an error.
    """
    try:
        df = pd.json_normalize(json_data, record_path)
        return df
    except ValueError:
        print("The data is not in the expected format")
        return None

def save_to_parquet(df, output_path):
    """
    Saves a DataFrame in Parquet format in the specified directory using fastparquet.

    Parameters:
    df (DataFrame): The DataFrame to save.
    path (str): The file path where the DataFrame will be saved.
    partition_cols (list, optional): Columns by which the data will be partitioned.
    """
    directory = os.path.dirname(output_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    df.to_parquet(output_path)

def get_last_extraction_date(file_path):
    """
    Retrieves the date of the last incremental extraction from a file.

    Parameters:
    file_path (str): The file path that contains the last extraction date.

    Returns:
    datetime: The date of the last extraction, or None if there's an error.
    """
    try:
        with open(file_path, 'r') as file:
            last_extraction_date = file.readline().strip()
            return datetime.strptime(last_extraction_date, '%Y-%m-%d')
    except (FileNotFoundError, ValueError):
        return None

def update_last_extraction_date(file_path, extraction_date):
    """
    Updates the date of the last incremental extraction in a file.

    Parameters:
    file_path (str): The file path where the extraction date will be saved.
    extraction_date (datetime): The date of the current extraction.
    """
    with open(file_path, 'w') as file:
        file.write(extraction_date.strftime('%Y-%m-%d'))

# def get_data(base_url, endpoint, params=None):
#     try:
#         all_results = []
       
#         while True:
#             response = requests.get(f"{base_url}/{endpoint}", params=params)
#             response.raise_for_status()
            
#             data = response.json()
#             results = data['data']['results']
#             all_results.extend(results)  # Almacena los resultados obtenidos
#             return all_results
#     except requests.exceptions.RequestException as e:
#         print(f"Error en la petición: {e}")
#         return None