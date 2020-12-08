#!/usr/bin/env python
# coding: utf-8

import logging
from logging.handlers import RotatingFileHandler
logging.captureWarnings(True)

logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
rHandler = RotatingFileHandler("ccgp.log",maxBytes = 10*1024*1024,backupCount = 1)
rHandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rHandler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

logger.addHandler(rHandler)
logger.addHandler(console)

