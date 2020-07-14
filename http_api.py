import time
import urllib.request
import urllib.error
from socket import timeout
import json
import gzip
import logging
import shutil
import io

from config import PROXY, ERROR_DELAY, DEBUG

logger = logging.getLogger(__name__)


def log(*args):
    s = " ".join([str(i) for i in args])
    logger.log(logging.INFO, s)


def load_with_retry(url, need_json=False, use_gzip=True):
    while True:
        log("request", url)
        try:
            if use_gzip == True:
                opener.addheaders = [("Accept-Encoding", "gzip")]
            urllib.request.install_opener(opener)
            response = urllib.request.urlopen(url, timeout=10)
            body = response.read()
            if response.headers.get('Content-Encoding') == 'gzip':
                body = gzip.decompress(body)
            body = decode_str(body)
            if need_json:
                body = json.loads(body)
            return body
        except KeyboardInterrupt as e:
            raise e
        except urllib.error.HTTPError as err:
            if err.code == 404:
                return ''
            else:
                logger.exception("unexpected")
                time.sleep(ERROR_DELAY)
        except timeout:
            log("timeout")
            time.sleep(ERROR_DELAY)
        except Exception:
            logger.exception("unexpected")
            time.sleep(ERROR_DELAY)
        finally:
            opener.addheaders = []
            urllib.request.install_opener(None)


def decode_str(body):
    try:
        body = body.decode()
    except:
        body = body.decode("cp1252")
    return body


def load_file(url, file_name, use_gzip=True):
    while True:
        try:
            log("load file: " + url + " -> " + file_name)
            if use_gzip == True:
                opener.addheaders = [("Accept-Encoding", "gzip")]
            urllib.request.install_opener(opener)
            result = urllib.request.urlretrieve(url, file_name)
            if use_gzip and result[1].get('Content-Encoding') != 'gzip':
                tf = file_name + '.tmp'
                gzip_file(file_name, tf)
                shutil.move(tf, file_name)
            log("done")
            return
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            logger.exception("wget failed")
            time.sleep(ERROR_DELAY)
        finally:
            opener.addheaders = []
            urllib.request.install_opener(None)


def gzip_file(ifn, ofn):
    block_size = 1024*1024
    with io.open(ifn, 'rb') as f_in:
        with gzip.open(ofn, 'wb') as f_out:
            while True:
                block = f_in.read(block_size)
                if len(block) == 0:
                    break
                f_out.write(block)
    return


# urllib setup
PROXIES = {} if PROXY is None else {
    'http': PROXY,
    'https': PROXY
}
debug = 2 if DEBUG else 0
https_handler = urllib.request.HTTPSHandler(debuglevel=debug)
http_handler = urllib.request.HTTPHandler(debuglevel=debug)
proxy_handler = urllib.request.ProxyHandler(PROXIES)
proxy_auth_handler = urllib.request.ProxyBasicAuthHandler()
opener = urllib.request.build_opener(proxy_handler, proxy_auth_handler, http_handler, https_handler)

