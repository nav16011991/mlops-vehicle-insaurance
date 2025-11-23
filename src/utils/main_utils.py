import os
import sys

import numpy as np
import dill
import yaml
from pandas import DataFrame
from src.exception import MyException
from src.logger import logging

def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns its contents as a dictionary.

    Args:
        file_path (str): The path to the YAML file.
    Returns:
        dict: The contents of the YAML file.
    """
    try:
        with open(file_path, 'rb') as file:
            content = yaml.safe_load(file)
        logging.info(f"YAML file {file_path} read successfully.")
        return content
    except Exception as e:
        logging.error(f"Error reading YAML file {file_path}: {e}")
        raise MyException(e, sys) from e
    
def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    """
    Writes a dictionary to a YAML file.
    Args:
        file_path (str): The path to the YAML file.
        content (object): The content to write to the YAML file.
        replace (bool): Whether to replace the file if it already exists.
    """
    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Existing file {file_path} removed.")
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w') as file:
            yaml.dump(content, file)
        
        logging.info(f"YAML file {file_path} written successfully.")
    except Exception as e:
        logging.error(f"Error writing YAML file {file_path}: {e}")
        raise MyException(e, sys) from e
    
def load_object(file_path: str) -> object:
    """
    Returns model/object from project directory.
    file_path: str location of file to load
    return: Obj
    """
    try:
        with open(file_path, "rb") as file_obj:
            obj = dill.load(file_obj)
        return obj
    except Exception as e:
        raise MyException(e, sys) from e

def save_numpy_array_data(file_path: str, array: np.array):
    """
    Save numpy array data to file
    file_path: str location of file to save
    array: np.array data to save
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, 'wb') as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise MyException(e, sys) from e


def load_numpy_array_data(file_path: str) -> np.array:
    """
    load numpy array data from file
    file_path: str location of file to load
    return: np.array data loaded
    """
    try:
        with open(file_path, 'rb') as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise MyException(e, sys) from e


def save_object(file_path: str, obj: object) -> None:
    logging.info("Entered the save_object method of utils")

    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)

        logging.info("Exited the save_object method of utils")

    except Exception as e:
        raise MyException(e, sys) from e

# def drop_columns(df: DataFrame, cols: list)-> DataFrame:

#     """
#     drop the columns form a pandas DataFrame
#     df: pandas DataFrame
#     cols: list of columns to be dropped
#     """
#     logging.info("Entered drop_columns methon of utils")

#     try:
#         df = df.drop(columns=cols, axis=1)

#         logging.info("Exited the drop_columns method of utils")
        
#         return df
#     except Exception as e:
#         raise MyException(e, sys) from e