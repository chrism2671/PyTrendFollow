import logging
import os
from config.settings import file_logger, console_logger

logging.basicConfig()

if file_logger['enabled']:
    log_dir = os.path.dirname(file_logger['file_name'])
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    logger.propagate = False
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if console_logger['enabled']:
        s_handler = logging.StreamHandler()
        s_handler.setLevel(console_logger['level'])
        s_handler.setFormatter(formatter)
        logger.addHandler(s_handler)
    if file_logger['enabled']:
        f_handler = logging.FileHandler(file_logger['file_name'])
        f_handler.setLevel(file_logger['level'])
        f_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
    return logger