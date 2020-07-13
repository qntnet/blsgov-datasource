import logging
from http_api import load_with_retry, load_file
import json
import datetime
from pyquery import PyQuery
import abc
import os
import shutil
import wget
import re
import time
import gzip
import zipfile
import io
from functools import cmp_to_key

from config import REGISTRATION_KEY, WORK_DIR, ERROR_DELAY

BASE_FILE_URL = 'https://download.bls.gov/pub/time.series/'
BASE_API_URL = 'https://api.bls.gov/publicAPI/v2/'

logger = logging.getLogger(__name__)
logger.level = logging.INFO


def log(*args):
    s = " ".join([str(i) for i in args])
    logger.log(logging.INFO, s)


def load_db_list():
    url = BASE_FILE_URL + 'overview.txt'
    txt = load_with_retry(url)
    txt = txt.split("\n")
    start = next(i for i in range(len(txt)) if txt[i].strip() == "LIST OF DATABASES")
    start = start + 2
    end = next(i for i in range(start, len(txt)) if txt[i].strip() == "")
    txt = txt[start:end]
    dbs = dict((l.strip().split()[0], l.strip().split(None, 1)[1]) for l in txt)

    url = BASE_API_URL + "surveys?registrationkey=" + REGISTRATION_KEY
    txt = load_with_retry(url)

    try:
        jsobj = json.loads(txt)
        log(jsobj)
        jsobj = jsobj['Results']['survey']
    except json.JSONDecodeError:
        txt = '[' + txt.split('"survey": [')[1].split(']')[0] + ']'
        jsobj = json.loads(txt)
        log(jsobj)

    dbs2 = dict((i['survey_abbreviation'], i['survey_name']) for i in jsobj)

    dbs = {**dbs, **dbs2}
    loaders_ids = [l.symbol for l in loaders]
    return dict(i for i in dbs.items() if i[0] in loaders_ids)


def get_loader(symbol):
    return next((l for l in loaders if l.symbol == symbol))


class AbstractDbLoader(abc.ABC):
    symbol = ''
    work_dir = ''
    file_prefix_delimiter = '.'

    def __init__(self, symbol):
        self.symbol = symbol
        self.work_dir = os.path.join(WORK_DIR, 'tmp', 'download', symbol)

    def get_last_modification(self):
        """ returns last modification date """
        ff = self.load_file_list()
        if len(ff) < 1:
            return datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)
        else:
            return max(f['modified'] for f in ff)

    @abc.abstractmethod
    def download(self):
        """ download data for parsing """
        pass

    def clear(self):
        """ clear loaded data """
        shutil.rmtree(self.work_dir)

    @abc.abstractmethod
    def parse_meta(self):
        """ returns dict with meta data """
        pass

    @abc.abstractmethod
    def parse_series(self):
        """ generator, returns parsed series """
        pass

    @abc.abstractmethod
    def parse_data(self):
        """ generator, returns parsed_data """
        pass

    @abc.abstractmethod
    def parse_aspect(self):
        """ generator, returns parsed_aspects """
        pass

    def load_file_list(self):
        try:
            sl = self.symbol.lower()
            file_url = BASE_FILE_URL + sl + "/"
            txt = load_with_retry(file_url)
            pq = PyQuery(txt)
            pq = pq('a')
            files = []
            for a in pq[1:]:
                name = a.attrib['href'].split('/')[-1]
                if not name.startswith(sl + self.file_prefix_delimiter):
                    continue
                prvtxt = PyQuery(a).prev()[0].tail
                prvtxt = prvtxt.split()
                f = {
                    "name": name[len(sl) + len(self.file_prefix_delimiter):],
                    "modified": datetime.datetime.strptime(prvtxt[0] + " " + prvtxt[1] + " " + prvtxt[2] + " +0000",
                                                           "%m/%d/%Y %H:%M %p %z", ),
                    "size": int(prvtxt[3])
                }
                files.append(f)
            return files
        except:
            return []

    def download_file(self, f, use_gzip):
        url = BASE_FILE_URL + self.symbol.lower() + "/" + self.symbol.lower() + self.file_prefix_delimiter + f['name']
        file_name = os.path.join(self.work_dir, f['name'] + ('.gz' if use_gzip else ''))
        if not os.path.exists(file_name):
            load_file(url, file_name, use_gzip)
        f['path'] = file_name
        f['open'] = (lambda mode : gzip.open(file_name, mode)) if use_gzip else (lambda mode : open(file_name, mode))


class StandardDbLoader(AbstractDbLoader):
    series_file = None
    aspect_files = []
    data_files = []
    txt_files = []
    dict_files = []

    def download(self):
        os.makedirs(self.work_dir, exist_ok=True)

        files = self.load_file_list()
        self.series_file = next(f for f in files if f['name'] == 'series')
        self.aspect_files = [f for f in files if f['name'] == 'aspect' or f['name'].startswith('aspect.')]
        self.data_files = [f for f in files if f['name'] == 'data' or f['name'].startswith('data.')]
        self.txt_files = [f for f in files if f['name'] == 'txt' or f['name'] == 'contacts']
        not_dicts = [self.series_file] + self.aspect_files + self.data_files + self.txt_files
        self.dict_files = [f for f in files if f != self.series_file and f not in not_dicts]

        for f in files:
             self.download_file(f, True)

    @staticmethod
    def read_txt(f):
        with f['open']('rb') as fd:
            b = fd.read()
        try:
            return b.decode("utf-8")
        except:
            return b.decode("cp1252")

    def parse_dict(self, f):
        txt = self.read_txt(f)
        if self.symbol == 'MW' and f['name'] == 'MapErrors':
            txt = "AM\nMW".join(txt.split("AMMW"))
        legacy = '\t' not in txt
        txt = txt.split("\n")
        if legacy:
            log("legacy mode")
            txt = [l.strip() for l in txt]
            txt = [re.split(r'\s\s+', l) for l in txt]
            # log(txt)
        else:
            txt = [l.split("\t") for l in txt]
        txt = [[c.strip() for c in l] for l in txt]

        # rm empty columns
        for l in txt:
            while len(l) > 0 and len(l[-1]) == 0:
                l.pop()
        txt = [l for l in txt if len(l) > 0]

        txt = [l for l in txt if len(l) >= 2]
        txt = [l for l in txt if not l[0].startswith("---") or l[1].startswith("---")]
        # log(txt)
        if max(len(r) for r in txt) == 2:
            if len(txt[0][0]) != len(txt[-1][0]):
                txt = txt[1:]
            rows = dict((l[0], l[1]) for l in txt)
        elif len(txt[0][0]) == 0 or len(txt[0][0]) != len(txt[-1][0]):
            for t in txt:
                while len(t) > 0 and len(t[-1]) == 0:
                    t.pop()
            hlen = max(len(t) for t in txt)
            header = ['column' + str(i) for i in range(hlen)]
            header[0] = 'id'
            if self.symbol == 'CD' and f['name'] == 'category':
                txt[0] = txt[0][1:]
            for i in range(len(txt[0])):
                if len(txt[0][i]) != 0:
                    header[i] = txt[0][i]
            txt = txt[1:]
            rows = dict((l[0], dict(zip(header, l))) for l in txt)
            # log("headers", header)
        else:
            # log("headless")
            rows = dict((l[0], l) for l in txt)
        return rows

    def parse_meta(self):
        log(self.symbol + ": parse meta")
        result = dict()
        for f in self.dict_files:
            result[f['name']] = self.parse_dict(f)
        for f in self.txt_files:
            result[f['name']] = self.read_txt(f)
        return result

    def parse_series(self):
        log(self.symbol + ": parse series")
        last = None
        with self.series_file['open']('rt') as f:
            header = f.readline()
            header = header.split('\t')
            header = [h.strip() for h in header]
            # log(header)
            while True:
                line = f.readline()
                if len(line) == 0:
                    break
                line = line.split('\t')
                line = [h.strip() for h in line]
                if len(line) < 2:
                    continue
                r = dict(zip(header, line))
                last = r
                yield r
        log(self.symbol + ": last series: " + str(last))

    def parse_data(self):
        data_files = sorted(self.data_files, key=cmp_to_key(file_cmp))
        for f in data_files:
            with f['open']('rt') as fd:
                log(self.symbol + ": parse " + f['name'])
                header = fd.readline()
                header = header.strip()
                header = header.split('\t')
                header = [h.strip() for h in header]
                while True:
                    line = fd.readline()
                    if len(line) == 0:
                        break
                    if len(line.strip()) == 0:
                        continue
                    # log(line)
                    line = line.split('\t')
                    line = [l.strip() for l in line]
                    line = dict(zip(header, line))
                    # log(line, header)
                    year = int(line['year'])
                    period = line['period']
                    footnote_codes = line.get('footnote_codes', '')
                    if len(footnote_codes) == 0:
                        footnote_codes = []
                        if line.get('cont_break') == 'Y':
                            footnote_codes.append('B')
                        if line.get('status') == 'P':
                            footnote_codes.append('P')
                        if line.get('footnote_exists') == 'Y':
                            footnote_codes.append('F')
                    else:
                        footnote_codes = footnote_codes.split(',')
                    value = line['value'].replace("$", "")
                    if value == '-' or value == '':
                        value = float('nan')
                    else:
                        value = float(value)

                    record = {
                        'series_id':line['series_id'],
                        'year': year,
                        'period': period,
                        'footnote_codes': footnote_codes,
                        'value': value
                    }
                    yield record
                    last = {'line':line, 'record':record}
                log(self.symbol + ": last record:" + str(last))

    def parse_aspect(self):
        aspect_files = sorted(self.aspect_files, key=cmp_to_key(file_cmp))
        for f in aspect_files:
            with f['open']('rt') as fd:
                log(self.symbol + ": parse " + f['name'])
                header = fd.readline()
                header = header.strip()
                header = header.split('\t')
                header = [h.strip() for h in header]
                while True:
                    line = fd.readline()
                    if len(line) == 0:
                        break
                    if len(line.strip()) == 0:
                        continue
                    # log(line)
                    line = line.split('\t')
                    line = [l.strip() for l in line]
                    line = dict(zip(header, line))
                    # log(line, header)
                    year = int(line['year'])
                    period = line['period']
                    aspect_type = line['aspect_type']
                    footnote_codes = line.get('footnote_codes', '')
                    if len(footnote_codes) == 0:
                        footnote_codes = []
                        if line.get('cont_break') == 'Y':
                            footnote_codes.append('B')
                        if line.get('status') == 'P':
                            footnote_codes.append('P')
                        if line.get('footnote_exists') == 'Y':
                            footnote_codes.append('F')
                    else:
                        footnote_codes = footnote_codes.split(',')
                    value = line['value'].replace("$", "")
                    if value == '-' or value == '':
                        value = float('nan')
                    else:
                        value = float(value)

                    record = {
                        'series_id':line['series_id'],
                        'year': year,
                        'period': period,
                        'aspect_type': aspect_type,
                        'value': value,
                        'footnote_codes': footnote_codes,
                    }
                    yield record
                    last = {'line':line, 'record':record}
                log(self.symbol + ": last record:" + str(last))


def file_cmp(f1, f2):
    n1 = f1['name'].split('.', 2)
    n2 = f2['name'].split('.', 2)
    if len(n1) == 0:
        return 0
    if len(n1[1]) > len(n2[1]):
        return -1
    elif len(n1[1]) < len(n2[1]):
        return 1
    if n1[1] > n2[1]:
        return -1
    elif n1[1] < n2[1]:
        return 1
    if len(n1) == 1:
        return 0
    if n1[2] < n2[2]:
        return -1
    elif n1[2] > n2[2]:
        return 1
    return 0


class ZipDbLoader(StandardDbLoader):

    def __init__(self, symbol):
        super().__init__(symbol)
        self.file_prefix_delimiter = '_'

    def download(self):
        os.makedirs(self.work_dir, exist_ok=True)

        zip_files = self.load_file_list()
        for f in zip_files:
            self.download_file(f, False)

        series_zip_file = next((f for f in zip_files if f['name'] == 'series.zip'))
        self.series_file = self.convert_zip_to_files(series_zip_file)[0]

        data_zip_file = next((f for f in zip_files if f['name'] == 'data.zip'))
        self.data_files = self.convert_zip_to_files(data_zip_file)

        meta_zip_file = next((f for f in zip_files if f['name'] == 'meta.zip'))
        self.dict_files = self.convert_zip_to_files(meta_zip_file)

    def convert_zip_to_files(self, zf):
        res = []
        with zipfile.ZipFile(zf['open']('rb'), 'r') as z:
            for i in z.infolist():

                def mk_opener(f, name):
                    def opener(mode):
                        z = zipfile.ZipFile(f['open']('rb'), 'r')
                        r = z.open(name, mode[0])
                        if 'b' not in mode:
                            r = io.TextIOWrapper(r)
                        return r
                    return opener

                res.append({
                    'name': i.filename[len(self.symbol) + len(self.file_prefix_delimiter):],
                    'open': mk_opener(zf, i.filename)
                })
        return res


loaders = [
    StandardDbLoader("AP"),
    StandardDbLoader("BD"),
    StandardDbLoader("CE"),
    StandardDbLoader("CI"),
    StandardDbLoader("CM"),
    StandardDbLoader("CS"),
    StandardDbLoader("CU"),
    StandardDbLoader("CW"),
    StandardDbLoader("CX"),
    StandardDbLoader("EI"),
    StandardDbLoader("FM"),
    StandardDbLoader("FW"),
    StandardDbLoader("IP"),
    StandardDbLoader("JT"),
    StandardDbLoader("LA"),
    StandardDbLoader("LE"),
    StandardDbLoader("LN"),
    StandardDbLoader("LU"),
    StandardDbLoader("MP"),
    StandardDbLoader("NB"),
    StandardDbLoader("ND"),
    StandardDbLoader("OE"),
    StandardDbLoader("PC"),
    StandardDbLoader("PR"),
    StandardDbLoader("SM"),
    StandardDbLoader("SU"),
    StandardDbLoader("WD"),
    StandardDbLoader("WM"),
    StandardDbLoader("WP"),
    StandardDbLoader("WS"),
    StandardDbLoader("EP"),
    StandardDbLoader("IS"),
    StandardDbLoader("TU"),

    StandardDbLoader("BG"),
    StandardDbLoader("BP"),
    StandardDbLoader("CC"),
    StandardDbLoader("CD"),
    StandardDbLoader("CF"),
    StandardDbLoader("CH"),
    StandardDbLoader("EB"),
    StandardDbLoader("EC"),
    StandardDbLoader("EE"),
    StandardDbLoader("FI"),
    StandardDbLoader("GG"),
    StandardDbLoader("GP"),
    StandardDbLoader("HC"),
    StandardDbLoader("HS"),
    StandardDbLoader("II"),
    StandardDbLoader("IN"),
    StandardDbLoader("JL"),
    StandardDbLoader("LI"),
    StandardDbLoader("ML"),
    StandardDbLoader("MU"),
    StandardDbLoader("MW"),
    StandardDbLoader("NC"),

    StandardDbLoader("NW"),
    StandardDbLoader("OR"),
    StandardDbLoader("PD"),
    StandardDbLoader("SA"),
    StandardDbLoader("SH"),
    StandardDbLoader("SI"),

    ZipDbLoader("EN")

    # "NL" - missed
    # "EW" - missed
    # "LF" - missed
    # "PF" - missed
    # "PI" - missed
]


if __name__ == "__main__":
    # log(load_db_list())
    # log(load_db_file_list("AP"))

    # log(load_db_list())

    # loader=StandardDbLoader('CI')
    loader=get_loader('BD')
    loader.download()
    meta = loader.parse_meta()
    log(meta)
    for s in loader.parse_series():
        pass

    for s in loader.parse_data():
        pass

    for s in loader.parse_aspect():
        pass
