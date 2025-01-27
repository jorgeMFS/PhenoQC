# src/logging_module.py

import logging
import os
import datetime

def setup_logging(log_file=None, mode='w'):
    """
    Sets up the logging configuration once per process invocation.

    If `log_file` is not specified, we automatically create a new log filename
    by appending the current date-time to 'phenoqc_' for the parent process.

    For child processes, we re-use the parent's `log_file` but with `mode='a'`.
    """
    # Remove any existing handlers so we don't get duplicates in the same process
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)

    # If no log_file specified, build one with a date-time stamp
    if not log_file:
        now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"phenoqc_{now_str}.log"

    log_path = os.path.join(logs_dir, log_file)

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s',
        filemode=mode  # 'w' by default for parent, 'a' for child
    )

    logging.info("Logging initialized! Writing to %s with mode=%s", log_path, mode)


def log_activity(message, level='info'):
    """
    Logs an activity message to the current log file.

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
