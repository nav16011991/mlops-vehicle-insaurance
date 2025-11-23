import json
import os
import sys

import pandas as pd
from pandas import DataFrame

from src.exception import MyException
from src.logger import logging
from src.utils.main_utils import read_yaml_file, write_yaml_file
from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from src.entity.config_entity import DataValidationConfig
from src.constants import SCHEMA_FILE_PATH

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, 
                 data_validation_config: DataValidationConfig):
        """
        Initializes the DataValidation class with the given configuration and artifacts.
        Args:
            data_ingestion_artifact (DataIngestionArtifact): The artifact from data ingestion step.
            data_validation_config (DataValidationConfig): The configuration for data validation.
        """
        try:
            logging.info(f"{'>>'*20} Data Validation {'<<'*20}")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.schema_info = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise MyException(e, sys) from e
    
    def validate_number_of_columns(self, dataframe: DataFrame) -> bool:
        """
        Validates if the number of columns in the dataframe matches the schema.
        Args:
            dataframe (DataFrame): The dataframe to validate.
        Returns:
            bool: True if the number of columns matches, False otherwise.
        """
        try:
            status = len(dataframe.columns) == len(self.schema_info['columns'])
            logging.info(f"Number of columns validation status: {status}")
            return status
        except Exception as e:
            raise MyException(e, sys) from e
        
    def is_columns_exist(self, dataframe: DataFrame) -> bool:
        """
        Checks if all required columns exist in the dataframe.
        Args:
            dataframe (DataFrame): The dataframe to check.
        Returns:
            bool: True if all required columns exist, False otherwise.
        """
        dataframe_columns = dataframe.columns
        missing_numeric_columns = []
        missing_categorical_columns = []
        try:
            for column in self.schema_info['numerical_columns']:
                if column not in dataframe_columns:
                    missing_numeric_columns.append(column)
            
            for column in self.schema_info['categorical_columns']:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)
            
            if len(missing_numeric_columns) > 0:
                logging.info(f"Missing numerical columns: {missing_numeric_columns}")
            if len(missing_categorical_columns) > 0:
                logging.info(f"Missing categorical columns: {missing_categorical_columns}")
            
            return False if len(missing_numeric_columns) > 0 or len(missing_categorical_columns) > 0 else True
        except Exception as e:
            raise MyException(e, sys) from e
    
    @staticmethod
    def read_data(file_path: str) -> DataFrame:
        """
        Reads a CSV file and returns a pandas DataFrame.
        Args:
            file_path (str): The path to the CSV file.
        Returns:
            DataFrame: The loaded pandas DataFrame.
        """
        try:
            dataframe = pd.read_csv(file_path)
            logging.info(f"Data read successfully from {file_path}")
            return dataframe
        except Exception as e:
            raise MyException(e, sys) from e
        
    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Initiates the data validation process.
        Returns:
            DataValidationArtifact: The artifact containing validation results.
        """
        try:
            logging.info("Starting data validation process")
            train_dataframe = self.read_data(self.data_ingestion_artifact.training_file_path)
            test_dataframe = self.read_data(self.data_ingestion_artifact.testing_file_path)
            
            validation_status = True
            message = ""
            # Validate training data
            if not self.validate_number_of_columns(train_dataframe):
                validation_status = False
                message += f"Training data does not have the expected number of columns"
                logging.error(message)
            
            if not self.is_columns_exist(train_dataframe):
                validation_status = False
                message += f"Training data is missing required columns"
                logging.error(message)
            
            # Validate testing data
            if not self.validate_number_of_columns(test_dataframe):
                validation_status = False
                message += f"Testing data does not have the expected number of columns"
                logging.error(message)
            
            if not self.is_columns_exist(test_dataframe):
                validation_status = False
                message += f"Testing data is missing required columns"
                logging.error(message)

            message += f"Data Validation Successful" if validation_status else f"Data Validation Failed"
            logging.info(message)
            
            # Write validation report
            report = {
                "validation_status": validation_status,
                "message": message
            }
            write_yaml_file(self.data_validation_config.validation_report_file_path, report, replace=True)
            logging.info("Data validation report written successfully")
            
            
            
            return DataValidationArtifact(
                validation_report_file_path=self.data_validation_config.validation_report_file_path,
                validation_status=validation_status,
                message=message
            )
        except Exception as e:
            raise MyException(e, sys) from e
