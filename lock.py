import portalocker

from config import LOCK_FILE


def shared_lock():
    return portalocker.Lock(LOCK_FILE, flags=portalocker.LOCK_SH | portalocker.LOCK_NB)


def exclusive_lock():
    return portalocker.Lock(LOCK_FILE, flags=portalocker.LOCK_EX)