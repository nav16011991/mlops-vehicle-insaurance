import sys
from src.exception import MyException
from src.logger import logging

from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation

from src.entity.config_entity import DataIngestionConfig
from src.entity.config_entity import DataValidationConfig

from src.entity.artifact_entity import DataIngestionArtifact
from src.entity.artifact_entity import DataValidationArtifact


class TrainingPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
    
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
        
    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact) -> DataValidationArtifact:
        """
        Starts the data validation process.
        
        Args:
            data_ingestion_artifact (DataIngestionArtifact): The artifact from data ingestion step.
        
        Returns:
        -------
        DataValidationArtifact
            The artifact produced by the data validation process.
        
        Raises:
        ------
        MyException
            If there is an issue during data validation.
        """
        try:
            logging.info("Starting data validation process")
            data_validation = DataValidation(
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_config=self.data_validation_config
            )
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info(f"Data validation completed successfully: {data_validation_artifact}")
            return data_validation_artifact
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
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            logging.info("Training pipeline executed successfully")
        except Exception as e:
            raise MyException(e, sys) from e