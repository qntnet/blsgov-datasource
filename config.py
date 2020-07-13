import os
import logging

logging.basicConfig(level=logging.INFO)

WORK_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "work")

ERROR_DELAY = 10

PROXY = os.getenv('PROXY')
REGISTRATION_KEY = os.getenv('REGISTRATION_KEY', '')

PORT = 5000
DEBUG = True

try:
    from config_local import *
except:
    pass



