import logging
import os
import sys

log_level = int(os.getenv("LOG_LEVEL", logging.INFO))
prefix = 'CASE_STUDY_APP'
fmt = logging.Formatter("%(asctime)s %(name)s(%(lineno)d) %(levelname)s: %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(fmt)
logger = logging.getLogger(prefix)
logger.setLevel(log_level)
logger.addHandler(handler)


def get_logger(app_name: str = None, log_path: str = None):
    """
    Returns a basic logger. This also makes sure 
    Argmuments:
        app_name : app name to be used for the logger
        log_path : path where logs needs to be stored [Optional]
    Output:
        Returns a logger object
    """
    pod_name = os.getenv('POD_NAME', 'local')
    name = f"{prefix}.{pod_name}"
    if app_name:
        name = f"{name}.{app_name}"
    logger = logging.getLogger(name)

    if log_path:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    return logger
