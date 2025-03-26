import datetime
import io
import logging
import os

PROXY = os.getenv('PROXY')
REGISTRATION_KEY = os.getenv('REGISTRATION_KEY', '31dab22a9f9d4010b5d89dc9e85f4025')

MODIFIED_LESS_THAN = datetime.timedelta(days=365 * 2)

logging.basicConfig(level=logging.INFO)

ERROR_DELAY = 10

DEBUG = os.getenv("DEBUG", 'true').lower() == 'true'

WORK_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "work")
WRK_DB_DIR = os.path.join(WORK_DIR, 'dbs')
DB_LIST_FILE_NAME = os.path.join(WRK_DB_DIR, 'list.json.gz')

META_FILE_NAME = 'meta.json.gz'

TMP_DB_DIR = os.path.join(WORK_DIR, 'tmp', 'dbs')

SERIES_PREFIX = "series."
DATA_PREFIX = "data."
ASPECT_PREFIX = "aspect."

JSON_SUFFIX='.json'
JSON_GZ_SUFFIX='.json.gz'
ZIP_SUFFIX='.zip'
FILE_NAME_DELIMITER='.'

LOCK_FILE = os.path.join(WORK_DIR, 'lock')

io.DEFAULT_BUFFER_SIZE = 512 * 1024

MAX_SERIES_PER_BATCH = 25000
MAX_DATA_PER_BATCH = 1000000

try:
    from config_local import *
except:
    pass



