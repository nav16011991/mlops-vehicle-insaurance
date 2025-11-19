import sys
import logging

def error_message_detail(error, error_detail: sys):
    """
    Constructs a detailed error message including the filename and line number where the error occurred.

    Args:
        error (Exception): The exception object.
        error_detail (sys): The sys module to extract traceback information.
    Returns:
        str: A formatted string containing the filename, line number, and error message.
    """

    # Extract traceback information
    _, _, exc_tb = error_detail.exc_info()
    # Get filename and line number from traceback
    file_name = exc_tb.tb_frame.f_code.co_filename
    line_number = exc_tb.tb_lineno
    # Format the error message
    error_message = f"Error occurred in file: {file_name} at line: {line_number} with message: {str(error)}"

    #Log the error message
    logging.error(error_message)

    return error_message

class MyException(Exception):
    """
    Custom exception class that extends the base Exception class.
    It captures detailed error information including filename and line number.
    """

    def __init__(self, error, error_detail: sys):
        """
        Initializes the MyException instance with a detailed error message.

        Args:
            error (Exception): The exception object.
            error_detail (sys): The sys module to extract traceback information.
        """
        # Get the detailed error message
        self.error_message = error_message_detail(error, error_detail)
        super().__init__(self.error_message)
    
    def __str__(self):
        """
        Returns the string representation of the exception.
        
        Returns:
            str: The detailed error message.
        """
        return self.error_message