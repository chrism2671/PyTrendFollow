import logging
import os
from config.settings import file_logger, console_logger, loggly_logger

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
    if loggly_logger['enabled']:
        from loggly.handlers import HTTPSHandler
        l_handler = HTTPSHandler('https://logs-01.loggly.com/inputs/%s/tag/python' %
                                 loggly_logger['api_key'])
        l_handler.setLevel(loggly_logger['level'])
        l_formatter = logging.Formatter('{"loggerName":"%(name)s", "asciTime":"%(asctime)s", "fileName":"%(filename)s",'
                                        ' "logRecordCreationTime":"%(created)f", "functionName":"%(funcName)s",'
                                        ' "levelNo":"%(levelno)s", "lineNo":"%(lineno)d", "time":"%(msecs)d",'
                                        ' "levelName":"%(levelname)s", "message":"%(message)s"}')
        l_handler.setFormatter(l_formatter)
        logger.addHandler(l_handler)
    return logger

# class ibpy_logger(object):
#     """IBPy's logger module is badly behaved. This monkey patch replaces it with defaults."""
#     def logger():
#         # l = logging.getLogger('ibpy')
#         # l.setLevel(logging.DEBUG)
#         return get_logger('ibpy')
# import ib.lib.logger
# ib.lib.logger = ibpy_logger