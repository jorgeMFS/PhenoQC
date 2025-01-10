import logging, os

def setup_logging(log_file='phenoqc.log'):
    """
    Sets up the logging configuration.

    Args:
        log_file (str): Filename for the log file.
    """
    # Force creation of a dedicated logs folder
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, log_file)

    # Remove any existing handlers so we don't get duplicates
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s',
        filemode='a'
    )

    logging.info("Logging initialized! Writing to %s", log_path)

def log_activity(message, level='info'):
    """
    Logs an activity message to the phenoqc.log file.

    Args:
        message (str): Message to log.
        level (str): Logging level ('info', 'warning', 'error', 'debug').
    """
    if level == 'info':
        logging.info(message)
    elif level == 'warning':
        logging.warning(message)
    elif level == 'error':
        logging.error(message)
    else:
        logging.debug(message)
