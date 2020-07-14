import json, gzip
from blsgov_api import load_db_list, get_loader
from config import WRK_DB_DIR, META_GZ_FILE_NAME, TMP_DB_DIR, DATA_PREFIX, ASPECT_PREFIX, \
    SERIES_PREFIX, JSON_GZ_SUFFIX, JSON_SUFFIX, ZIP_SUFFIX, DB_LIST_FILE_NAME, MAX_SERIES_PER_BATCH, \
    MAX_DATA_PER_BATCH
import os, sys
import shutil
import logging
import zipfile
import itertools

from lock import exclusive_lock

TMP_PREFIX = 'tmp.'
logger = logging.getLogger(__name__)


def log(*args):
    s = " ".join([str(i) for i in args])
    logger.log(logging.INFO, s)


def update_dbs(symbols = None, force_all=False):
    log('load db lists')

    new_db_list = load_db_list()
    cur_db_list = []
    try:
        with gzip.open(DB_LIST_FILE_NAME, 'rt') as f:
            cur_db_list = json.loads(f.read())
    except:
        pass

    for ndb in new_db_list:
        if symbols is not None and ndb['symbol'] not in symbols:
            continue
        cdb = next((i for i in cur_db_list if i['symbol'] == ndb['symbol']), None)
        if cdb is None or cdb['modified'] < ndb['modified'] or force_all: # check corrupted files
            if cdb is not None:
                cur_db_list.remove(cdb)
            cur_db_list.append(ndb)
            updater = Updater(ndb['symbol'])
            updater.prepare_update()

            with exclusive_lock():
                updater.update()
                with gzip.open(DB_LIST_FILE_NAME, 'wt') as f:
                    f.write(json.dumps(cur_db_list, indent=1))


class Updater:

    def __init__(self, symbol):
        self.symbol = symbol
        self.loader = get_loader(symbol)
        self.tmp_dir = os.path.join(TMP_DB_DIR, self.symbol.lower())
        self.wrk_dir = os.path.join(WRK_DB_DIR, self.symbol.lower())
        self.batch_size = 1

    def update(self):
        log(self.symbol + ": update")
        try:
            shutil.rmtree(self.wrk_dir)
        except FileNotFoundError:
            pass
        os.makedirs(os.path.dirname(self.wrk_dir), exist_ok=True)
        shutil.move(self.tmp_dir, self.wrk_dir)

    def prepare_update(self):
        log(self.symbol + ": prepare update")
        try:
            shutil.rmtree(self.tmp_dir)
        except FileNotFoundError:
            pass
        os.makedirs(self.tmp_dir, exist_ok=True)

        self.loader.download()

        log(self.symbol + ": calc batch size")
        series_count = self.loader.approx_series_count()
        data_count = self.loader.approx_data_count()
        s_batch_count = series_count // MAX_SERIES_PER_BATCH + 1
        d_batch_count = data_count // MAX_DATA_PER_BATCH + 1
        batch_count = max(s_batch_count, d_batch_count, 1)
        self.batch_size = series_count//batch_count
        log(self.symbol + ":", "batch_size:", self.batch_size, "batch_count:", batch_count)

        self.update_meta()
        self.update_series_list()

        self.update_data_series(DATA_PREFIX, self.loader.parse_data())
        self.update_data_series(ASPECT_PREFIX, self.loader.parse_aspect())

        self.loader.clear()

    def update_meta(self):
        log(self.symbol + ": update meta")
        # load meta
        meta = self.loader.parse_meta()
        meta_fn = os.path.join(self.tmp_dir, META_GZ_FILE_NAME)
        with gzip.open(meta_fn, 'wt') as f:
            f.write(json.dumps(meta, indent=1))

    def update_series_list(self):
        log(self.symbol + ": update series")
        # load series
        batch = []
        batch_files = []
        i = 0

        def write_series_batch():
            fn = os.path.join(self.tmp_dir, TMP_PREFIX + SERIES_PREFIX + str(i) + JSON_GZ_SUFFIX)
            batch_files.append(fn)
            batch.sort(key=lambda b:b['series_id'])
            with gzip.open(fn, 'wt') as f:
                for b in batch:
                    f.write(json.dumps(b) + "\n")

        for s in self.loader.parse_series():
            batch.append(s)
            if len(batch) >= self.batch_size:
                write_series_batch()
                i += 1
                batch = []
        if len(batch) > 0:
            write_series_batch()

        log("build sorted index")

        def sorted_series_generator():
            fds = [{"file": gzip.open(b, 'rt'), 'cur': None} for b in batch_files]
            while True:
                closed = False
                for fd in fds:
                    if fd['cur'] is None and fd['file'] is not None:
                        row = fd['file'].readline()
                        if len(row) == 0:
                            fd['file'].close()
                            fd['file'] = None
                            closed = True
                        else:
                            fd['cur'] = json.loads(row)
                if closed:
                    fds = [fd for fd in fds if fd['file'] is not None]
                    if len(fds) == 0:
                        break
                mx = min(fds, key=lambda fd: fd['cur']['series_id'])
                yield mx['cur']
                mx['cur'] = None

        batch = []
        for s in sorted_series_generator():
            batch.append(s)
            if len(batch) >= self.batch_size:
                fn = os.path.join(self.tmp_dir, SERIES_PREFIX + batch[0]['series_id'] + '.' + batch[-1]['series_id'] + JSON_GZ_SUFFIX)
                with gzip.open(fn, 'wt') as f:
                    f.write(array_to_json(batch))
                i += 1
                batch = []
        if len(batch) > 0:
            fn = os.path.join(self.tmp_dir, SERIES_PREFIX + batch[0]['series_id'] + '.' + batch[-1]['series_id'] + JSON_GZ_SUFFIX)
            with gzip.open(fn, 'wt') as f:
                f.write(array_to_json(batch))

        for bf in batch_files:
            os.remove(bf)

    def update_data_series(self, prefix, data_source_generator):
        log(self.symbol + ":update data " + prefix)
        batch_files = []
        for fn in os.listdir(self.tmp_dir):
            if fn.startswith(SERIES_PREFIX):
                nfp = fn.split('.')
                batch_fn = os.path.join(self.tmp_dir, TMP_PREFIX + prefix + nfp[1] + '.' + nfp[2] + JSON_GZ_SUFFIX)
                batch_files.append({
                    'from': nfp[1],
                    'to': nfp[2],
                    'path': batch_fn,
                    'fd': gzip.open(batch_fn, 'wt'),
                })
        for s in data_source_generator:
            bf = next((bf for bf in batch_files if bf['from'] <= s['series_id'] <= bf['to']))
            bf['fd'].write(json.dumps(s) + "\n")

        log("transform gz to zip")

        for bf in batch_files:
            bf['fd'].close()

            with gzip.open(bf['path'], 'rt') as f:
                data = f.read()
            data = data.split('\n')[:-1]
            if len(data) > 0:
                data = '[\n' + ',\n'.join(data) + ']'
                data = json.loads(data)
                data.sort(key=lambda i: (i['series_id'], i['year'], i['period']))

                zip_file_name = os.path.join(self.tmp_dir, prefix + bf['from'] + '.' + bf['to'] + ZIP_SUFFIX)
                with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
                    for s in itertools.groupby(data, key=lambda i:i['series_id']):
                        # rm duplicates
                        series = [next(i[1]) for i in itertools.groupby(s[1], key=lambda i: (i['year'], i['period']))]
                        for i in series:
                            del i['series_id']
                        series_fn = s[0] + JSON_SUFFIX
                        z.writestr(series_fn, array_to_json(series))

            os.remove(bf['path'])


def array_to_json(arr):
    return "[\n" + ",\n".join([json.dumps(a) for a in arr]) + "\n]"


if __name__ == '__main__':
    log(sys.argv)
    force = '-f' in sys.argv
    all = '-a' in sys.argv
    symbols = [i for i in sys.argv if not i.startswith('-')]
    update_dbs(symbols if not all else None, force)