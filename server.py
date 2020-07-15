import gzip
import json
import os
import zipfile

from flask import Flask, jsonify, safe_join, send_file, request
from werkzeug.exceptions import NotFound
from werkzeug.middleware.proxy_fix import ProxyFix

from config import DEBUG, WRK_DB_DIR, DB_LIST_FILE_NAME, META_FILE_NAME, SERIES_PREFIX, FILE_NAME_DELIMITER, JSON_SUFFIX
from lock import shared_lock

app = Flask("blsgov-datasource")
app.wsgi_app = ProxyFix(app.wsgi_app)


@app.route('/api/files/<path>')
@app.route('/api/files/')
def get_files(path=''):
    with shared_lock():
        path = safe_join(WRK_DB_DIR, path) if len(path) > 0 else WRK_DB_DIR
        if not os.path.exists(path):
            raise NotFound()
        if os.path.isdir(path):
            lst = os.listdir(path)
            lst = [{"name": i, "type": 'dir' if os.path.isdir(os.path.join(path, i)) else 'file'} for i in lst]
            return jsonify(lst)
        else:
            return send_file(path)


@app.route('/api/db/')
@app.route('/api/db/<db_id>')
def get_db_list(db_id=None):
    with shared_lock():
        with gzip.open(DB_LIST_FILE_NAME, 'rt') as f:
            dbs = f.read()
        dbs = json.loads(dbs)
        if db_id is not None:
            db = next((d for d in dbs if d['id'] == db_id), None)
            if db is None:
                raise NotFound()
            jsonify(db)
        return jsonify(dbs)


@app.route('/api/db/<db_id>/meta')
def get_meta(db_id=None):
    with shared_lock():
        path = safe_join(WRK_DB_DIR, db_id.lower())
        path = os.path.join(path, META_FILE_NAME)
        if not os.path.exists(path) or not os.path.isfile(path):
            raise NotFound()
        with gzip.open(path, 'rt') as f:
            data = f.read()
        data = json.loads(data)
        return jsonify(data)


@app.route('/api/db/<db_id>/series/')
@app.route('/api/db/<db_id>/series/<series_id>')
def get_series(db_id=None, series_id=None):
    with shared_lock():
        last_series_id = request.args.get('after')
        db_path = safe_join(WRK_DB_DIR, db_id.lower())

        files = os.listdir(db_path)
        files = [{
            "from": f.split(FILE_NAME_DELIMITER)[1],
            "to": f.split(FILE_NAME_DELIMITER)[2],
            "name": f
        } for f in files if f.startswith(SERIES_PREFIX)]
        files.sort(key=lambda f: f['from'])
        if series_id is not None:
            series_file = next((f for f in files if f['from'] <= series_id <= f['to']), None)
        elif last_series_id is None:
            series_file = files[0]
        else:
            series_file = next((f for f in files if f['to'] > last_series_id), None)
        if series_file is None:
            return jsonify([])

        series_path = os.path.join(db_path, series_file['name'])
        with gzip.open(series_path, 'rt') as f:
            series = f.read()
        series = json.loads(series)

        if series_id is not None:
            series = next((s for s in series if s['id'] == series_id), None)
            if series is None:
                raise NotFound()
            return jsonify(series)

        if last_series_id is not None:
            series = [s for s in series if s['id'] > last_series_id]

        return {
            'data': series,
            'next_page': None if files.index(series_file) >= len(files) - 1 else ('?after=' + series[-1]['id'])
            # TODO better pagination: count, offset, limit
        }


@app.route('/api/db/<db_id>/series/<series_id>/<kind>')
def get_data(db_id, series_id=None, kind=None):
    with shared_lock():
        prefix = kind + FILE_NAME_DELIMITER
        db_path = safe_join(WRK_DB_DIR, db_id.lower())
        files = os.listdir(db_path)
        files = [{
            "from": f.split(FILE_NAME_DELIMITER)[1],
            "to": f.split(FILE_NAME_DELIMITER)[2],
            "name": f
        } for f in files if f.startswith(prefix)]
        data_file = next((f for f in files if f['from'] <= series_id <= f['to']), None)
        if data_file is None:
            raise NotFound()
        path = os.path.join(db_path, data_file['name'])
        with zipfile.ZipFile(path, 'r') as z:
            try:
                content = z.read(series_id + JSON_SUFFIX)
            except KeyError:
                raise NotFound()
        content = content.decode()
        content = json.loads(content)
        return jsonify(content)


app.debug = DEBUG

if __name__ == '__main__':
    app.run(host='0.0.0.0')
