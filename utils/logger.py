import logging
from pathlib import Path
from typing import Optional, Union


def get_logger(
    logOutputFilename: Optional[Union[str, Path]] = None,
    overwriteExistingFile: bool = False,
) -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(level=logging.INFO)
    loggingFormatter = logging.Formatter(f"%(asctime)s - %(levelname)s - %(message)s")
    streamHandler = logging.StreamHandler(stream=None)
    streamHandler.setFormatter(fmt=loggingFormatter)
    logger.addHandler(hdlr=streamHandler)
    if logOutputFilename is not None:
        fileHandler = logging.FileHandler(
            filename=logOutputFilename, mode="w" if overwriteExistingFile else "a"
        )
        fileHandler.setFormatter(fmt=loggingFormatter)
        logger.addHandler(hdlr=fileHandler)
    return logger
