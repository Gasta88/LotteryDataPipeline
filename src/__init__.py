#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to import the src module."""

import logging

def setup_logging(logFile=None, cmdline=True, level='info'):
    """Set up the logging file and the level of information displayed.

    Parameters
    ----------
    logFile : str
        Location of the log file stored inside the configuration file.

    cmdLine: boolean
        add a stream handler to log to the command line (if True)
    level : str
        Logging level of information.

    Returns
    -------
    logging: object
        Handle to the log file where to write info during the process.

    """
    level = {'debug': logging.DEBUG,
             'info': logging.INFO,
             'warning': logging.WARNING,
             'error': logging.ERROR}.get(level.lower())

    form = ('%(asctime)s: [%(levelname)s]: %(filename)s in '
            '%(funcName)s (%(lineno)d): %(message)s')
    logger = logging.getLogger('file_logger')
    logger.setLevel(level)
    if cmdline:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(form))
        ch.setLevel(level)
        logger.addHandler(ch)
        logger.debug('setup commandline logging level: ' + str(level))

    if logFile is not None:
        eh = logging.FileHandler(logFile, mode='w')
        eh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(form)
        eh.setFormatter(formatter)
        logger.addHandler(eh)
        logger.debug('Set up file logging: ' + logFile)

    return logger

