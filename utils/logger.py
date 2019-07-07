import logging
from datetime import datetime
import os
import sys

def create_logger():
    # create logger for "Sample App"
    logger = logging.getLogger('afd_download')
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    now = datetime.now()
    if not os.path.exists('./logs'):
      os.makedirs('./logs')
    fh = logging.FileHandler('logs/getAFD.log', mode='a')
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level 
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    file_formatter = logging.Formatter('[%(asctime)s] | %(levelname)8s: %(message)s ' +
                                '(%(filename)s:%(lineno)s)', datefmt='%Y-%m-%d %H:%M:%S')
    std_out_formatter = logging.Formatter('[%(asctime)s] | %(levelname)8s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh.setFormatter(file_formatter)
    ch.setFormatter(std_out_formatter)

    # add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger