import os
import sys
import pymongo
import certifi

from src.exception import MyException
from src.logger import logging
from src.constants import DATABASE_NAME, MONGODB_URL_KEY

#Load the certificate authority file to avoid timeout errrors when connecting to MongoDB Atlas
CA = certifi.where()

class MongoDBClient:
    """
    A class to manage MongoDB connection and provide access to the database.

    Attributes:
        client (pymongo.MongoClient): The MongoDB client instance.
        database (pymongo.database.Database): The MongoDB database instance.
    
    Methods:
         __init__(database_name: str) -> None
        Initializes the MongoDB connection using the given database name.
    
    """
    client = None

    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        """
        Initializes a connection to the MongoDB database. If no existing connection is found, it establishes a new one.
        
        Parameters:
        ----------
        database_name : str, optional
            Name of the MongoDB database to connect to. Default is set by DATABASE_NAME constant.

        Raises:
        ------
        MyException
            If there is an issue connecting to MongoDB or if the environment variable for the MongoDB URL is not set.
        """
        try:
            if MongoDBClient.client is None:
                mongodb_url = os.getenv(MONGODB_URL_KEY)
                if not mongodb_url:
                    raise MyException(f"Environment variable '{MONGODB_URL_KEY}' not set.", sys)
                
                MongoDBClient.client = pymongo.MongoClient(mongodb_url, tlsCAFile=CA)
                logging.info("MongoDB connection established successfully.")
            
             # Use the shared MongoClient for this instance
            self.client = MongoDBClient.client
            self.database = self.client[database_name]  # Connect to the specified database
            self.database_name = database_name
            logging.info("MongoDB connection successful.")
        except Exception as e:
            raise MyException(f"Error connecting to MongoDB: {e}", sys) from e
