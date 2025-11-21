import os
import sys

from pandas import DataFrame
from sklearn.model_selection import train_test_split

from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact
from src.exception import MyException
from src.logger import logging
from src.data_access.vehicle_insuarance_data import VehicleInsuranceData

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig = DataIngestionConfig()):
        """
        Initializes the DataIngestion component with the given configuration.
        
        Parameters:
        ----------
        data_ingestion_config : DataIngestionConfig
            Configuration for data ingestion process.
        Raises:
        ------
        MyException
            If there is an issue during initialization.
        """
        try:
            logging.info(f"{'>>'*20} Data Ingestion {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise MyException(e, sys) from e

    def export_data_into_feature_store(self) -> DataFrame:
        """
        Exports data from MongoDB collection into a feature store as a CSV file.
        
        Returns:
        -------
        DataFrame
            The exported data as a pandas DataFrame.
        
        Raises:
        ------
        MyException
            If there is an issue during data export.
        """
        try:
            logging.info("Exporting data from MongoDB to feature store")
            vehicle_insurance_data = VehicleInsuranceData()
            df: DataFrame = vehicle_insurance_data.get_vehicle_insurance_data_as_dataframe(collection_name=
                                                                   self.data_ingestion_config.collection_name)
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            
            # Create feature store directory if it doesn't exist
            feature_store_dir = os.path.dirname(feature_store_file_path)
            os.makedirs(feature_store_dir, exist_ok=True)
            
            # Save the DataFrame to CSV
            df.to_csv(feature_store_file_path, index=False)
            logging.info(f"Data exported to feature store at: {feature_store_file_path}")
            return df
        except Exception as e:
            raise MyException(e, sys) from e
        
    
    def split_data_as_train_test(self, df: DataFrame) -> None:
        """
        Splits the DataFrame into training and testing sets and saves them as CSV files.
        
        Parameters:
        ----------
        df : DataFrame
            The input DataFrame to be split.
        
        Raises:
        ------
        MyException
            If there is an issue during data splitting or saving.
        """
        try:
            logging.info("Splitting data into train and test sets")
            train_set, test_set = train_test_split(
                df,
                test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info("Data split into train and test sets successfully")

            logging.info("Saving train and test sets to feature store")
            # Save the train and test sets to CSV files
            train_file_path = self.data_ingestion_config.training_file_path
            test_file_path = self.data_ingestion_config.testing_file_path
            
            logging.info(f"Saving train data at: {train_file_path}")
            os.makedirs(os.path.dirname(train_file_path), exist_ok=True)
            logging.info(f"Saving test data at: {test_file_path}")
            os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
            
            logging.info("Saving train and test data to CSV files")
            train_set.to_csv(train_file_path, index=False, header=True)
            test_set.to_csv(test_file_path, index=False, header=True)
            
            logging.info(f"Train and test data saved at: {train_file_path} and {test_file_path}")
        except Exception as e:
            raise MyException(e, sys) from e
        
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Initiates the data ingestion process: exports data, splits it, and saves the train/test sets.
        
        Returns:
        -------
        DataIngestionArtifact
            Artifact containing paths to the train and test data files.
        
        Raises:
        ------
        MyException
            If there is an issue during the data ingestion process.
        """
        try:
            logging.info("Starting data ingestion process")
            df = self.export_data_into_feature_store()
            logging.info("Exported data from MongoDB to feature store successfully")

            logging.info("Splitting data into train and test sets")
            self.split_data_as_train_test(df=df)
            logging.info("Data split into train and test sets successfully")
            
            
            data_ingestion_artifact = DataIngestionArtifact(
                training_file_path=self.data_ingestion_config.training_file_path,
                testing_file_path=self.data_ingestion_config.testing_file_path
            )
            logging.info(f"Data ingestion completed successfully: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise MyException(e, sys) from e