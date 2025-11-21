import sys
import pandas as pd
import numpy as np
from typing import Optional

from src.configuration.mongo_db_connection import MongoDBClient
from src.constants import DATABASE_NAME
from src.exception import MyException

class VehicleInsuranceData:
    """
    A class to handle vehicle insurance data retrieval from MongoDB to DataFrame.

    """

    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        """
        Initializes the VehicleInsuranceData with a MongoDB connection.

        Parameters:
        ----------
        database_name : str, optional
            Name of the MongoDB database to connect to. Default is set by DATABASE_NAME constant.
        """
        try:
            self.mongo_client = MongoDBClient(database_name=database_name)
        except Exception as e:
            raise MyException(f"Error initializing VehicleInsuranceData: {e}", sys) from e
        
    def get_vehicle_insurance_data_as_dataframe(self, collection_name: str) -> pd.DataFrame:
        """
        Retrieves vehicle insurance data from the specified MongoDB collection and converts it to a pandas DataFrame.

        Parameters:
        ----------
        collection_name : str
            Name of the MongoDB collection to retrieve data from.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the vehicle insurance data.
        
        Raises:
        ------
        MyException
            If there is an issue retrieving data from MongoDB or converting it to DataFrame.
        """
        try:
            # create a database collection object
            collection = self.mongo_client.database[collection_name]
            # retrieve all data from collection and convert to DataFrame
            data = list(collection.find())
            # Check if data is empty
            if not data:
                raise MyException(f"No data found in collection: {collection_name}", sys)
            
            # Convert list of documents to DataFrame
            df = pd.DataFrame(data)

            # Drop the MongoDB generated _id column if it exists
            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)
            
            # Replace "na" string with np.nan
            df.replace({"na":np.nan},inplace=True)
            
            return df
        except Exception as e:
            raise MyException(f"Error retrieving data from MongoDB: {e}", sys) from e