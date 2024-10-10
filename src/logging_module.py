import logging

def setup_logging(log_file='phenoqc.log'):
    """
    Sets up the logging configuration.
    
    Args:
        log_file (str): Path to the log file.
    """
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s',
        filemode='a'
    )

def log_activity(message, level='info'):
    """
    Logs an activity message.
    
    Args:
        message (str): Message to log.
        level (str): Logging level ('info', 'warning', 'error').
    """
    if not logging.getLogger().hasHandlers():
        setup_logging()
    
    if level == 'info':
        logging.info(message)
    elif level == 'warning':
        logging.warning(message)
    elif level == 'error':
        logging.error(message)
    else:
        logging.debug(message)