import sys
from src.exception import MyException
from src.logger import logging

from src.components.data_ingestion import DataIngestion

from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact

class TrainingPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
    
    def start_data_ingestion(self) -> DataIngestionArtifact:
        """
        Starts the data ingestion process.
        
        Returns:
        -------
        DataIngestionArtifact
            The artifact produced by the data ingestion process.
        
        Raises:
        ------
        MyException
            If there is an issue during data ingestion.
        """
        try:
            logging.info("Starting data ingestion process")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info(f"Data ingestion completed successfully: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise MyException(e, sys) from e

    def run_pipeline(self)-> None:
        """
        Runs the entire training pipeline.
        
        Raises:
        ------
        MyException
            If there is an issue during the pipeline execution.
        """
        try:
            logging.info("Running the training pipeline")
            data_ingestion_artifact = self.start_data_ingestion()
            # Further steps like data validation, transformation, model training can be added here
            logging.info("Training pipeline executed successfully")
        except Exception as e:
            raise MyException(e, sys) from e