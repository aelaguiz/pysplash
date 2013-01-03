# -*- coding: utf-8 -*-

from logging import getLogger, StreamHandler, Formatter, getLoggerClass, DEBUG, INFO
import logging.handlers
import time

logger_name = 'pysplash'


global_debug = False
global_logobj = None


def set_debug(debug=True):
    global global_logobj
    global global_debug

    global_debug = debug

    # If we are toggling global_debug flag and
    # a logger is already created, must replace it
    if global_logobj:
        global_logobj = create_logger()


def logger():
    global global_logobj

    if not global_logobj:
        global_logobj = create_logger()

    return global_logobj


def create_logger():
    Logger = getLoggerClass()

    class DebugFormatter(Formatter):
        def format(self, record):
            return "%s %s [%s] >> %s %s" % (
                time.strftime("%H:%M:%S", time.localtime(record.created)),
                ("%s %s" % (record.levelname, record.module)).ljust(15)[:15],
                ("%s:%d@%s" % (record.filename, record.lineno, record.funcName)).ljust(30)[:30],
                record.msg,
                ("\nException: %s" % self.formatException(record.exc_info)) if record.exc_info else "")

    import socket
    hostname = socket.gethostname()

    fileHandler = logging.handlers.RotatingFileHandler(
        'pysplash.%s.log' % hostname, mode='a', maxBytes=100000000, backupCount=25)

    fileHandler.doRollover()
    fileHandler.setFormatter(DebugFormatter())

    handler = StreamHandler()
    handler.setFormatter(DebugFormatter())
    logger = getLogger(logger_name)

    # just in case that was not a new logger, get rid of all the handlers
    # already attached to it.
    del logger.handlers[:]

    if global_debug:
        logger.setLevel(DEBUG)
    else:
        logger.setLevel(INFO)

    logger.addHandler(handler)
    logger.addHandler(fileHandler)
    return logger
