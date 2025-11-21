# Below code is added to demo.py to test logging functionality
# from src.loggor import logging
# logging.info("This is an info message from demo.py")
# logging.debug("This is a debug message from demo.py")
# logging.error("This is an error message from demo.py")
# logging.warning("This is a warning message from demo.py")
# logging.critical("This is a critical message from demo.py")




# Below code is added to demo.py to test logging functionality
# from src.exception import MyException
# from src.logger import logging
# import sys

# try:
#     logging.info("Starting to test logging and exception handling.")
#     # Intentionally cause a ZeroDivisionError
#     result = 10 + 'a'
# except Exception as e:
#     logging.error("An error occurred")
#     raise MyException(e, sys) from e


from src.pipeline.training_pipeline import TrainingPipeline
if __name__ == "__main__":
    training_pipeline = TrainingPipeline()
    training_pipeline.run_pipeline()