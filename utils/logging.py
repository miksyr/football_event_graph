import logging


def get_logger(outputDirectory, fileName=None, overwriteExistingFile=False):
    logger = logging.getLogger()
    logger.setLevel(level=logging.INFO)
    loggingFormatter = logging.Formatter(f'%(asctime)s - %(levelname)s - %(message)s')
    streamHandler = logging.StreamHandler(stream=None)
    streamHandler.setFormatter(fmt=loggingFormatter)
    logger.addHandler(hdlr=streamHandler)
    if fileName is not None:
        fileHandler = logging.FileHandler(filename=f'{outputDirectory}/{fileName}', mode='w' if overwriteExistingFile else 'a')
        fileHandler.setFormatter(fmt=loggingFormatter)
        logger.addHandler(hdlr=fileHandler)
    return logger
