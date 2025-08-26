import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from typing import Tuple, Dict, Optional

def gen_logger(name: str,
               name_abbr: str,
               max_bytes_per_log: int,
               max_backups: int) -> logging.Logger:
    '''set up logger object, return logger with separate progress/error tracking
    
    :param name: name of logger and of directory where logs will be stored
    :param name_abbr: name abbreviation, used for log file names
    :param max_bytes_per_log: max byte count per log file
    :param max_backups: max number of log files
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # make dirs
    os.makedirs('logs', exist_ok=True)
    os.makedirs(os.path.join('logs', name), exist_ok=True)

    # progress statements; e.g., time-to-complete batch #5
    PROG_PATH = os.path.join('logs', name, f'{name_abbr}_prog.log')
    prog_handler = RotatingFileHandler(filename=PROG_PATH,
                                       maxBytes=max_bytes_per_log,
                                       backupCount=max_backups)
    prog_handler.setLevel(logging.INFO)
    prog_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s := %(message)s',
                                 datefmt='%m-%d-%Y %H:%M:%S')
    prog_handler.setFormatter(prog_fmt)

    # stream to stdout
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s := %(message)s',
                                   datefmt='%m-%d-%Y %H:%M:%S')
    stream_handler.setFormatter(stream_fmt)

    # error statements; e.g., book ID 7777777 failed
    ERR_PATH = os.path.join('logs', name, f'{name_abbr}_err.log')
    err_handler = RotatingFileHandler(filename=ERR_PATH,
                                      maxBytes=max_bytes_per_log,
                                      backupCount=max_backups)
    err_handler.setLevel(logging.ERROR)
    err_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s := %(message)s',
                                datefmt='%m-%d-%Y %H:%M:%S')
    err_handler.setFormatter(err_fmt)

    logger.addHandler(prog_handler)
    logger.addHandler(err_handler)
    logger.addHandler(stream_handler)
    return logger
    

def update_sem_and_delay(current_sem_count: int,
                         current_sub_batch_delay: int,
                         timeouts_per_batch_ratio: float,
                         cfg: Optional[Dict[str,int]]) -> Tuple[int,int]:
    '''takes in batch metadata, returns new SEMAPHORE COUNT & SUB_BATCH_DELAY
    
    :param current_sem_count: current semaphore count
    :param current_sub_batch_delay: current delay in between sub-batches
    :param timeouts_per_batch_ratio: ratio of timeouts per batch pulls; e.g., # timeouts / # tasks
    :param cfg: sem/delay config; includes keys: MIN_SEM, MAX_SEM, MIN_DELAY, MAX_DELAY, RATIO_THRESHOLD, DELAY_DELTA
    '''
    if not cfg:
        cfg = {
            'MIN_SEM': 2,
            'MAX_SEM': 9,
            'MIN_DELAY': .25,
            'MAX_DELAY': 5,
            'RATIO_THRESHOLD': .05,
            'DELAY_DELTA': .1
        }

    scalar = -1 if timeouts_per_batch_ratio > cfg['RATIO_THRESHOLD'] else 1
    
    new_sem_count = (lambda x: sorted([cfg['MIN_SEM'], 
                                       current_sem_count + x, 
                                       cfg['MAX_SEM']])[1])(scalar)
    
    new_delay_count = (lambda x: sorted([cfg['MIN_DELAY'], 
                                         current_sub_batch_delay - cfg['DELAY_DELTA'] * x, 
                                         cfg['MAX_DELAY']])[1])(scalar)
    
    return new_sem_count, new_delay_count