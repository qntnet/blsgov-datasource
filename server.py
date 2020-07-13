from flask import Flask, jsonify, request
from config import PORT, DEBUG

app = Flask("blsgov-datasource")


@app.route('/<id>')
def hello_world(id):
    return jsonify({'Hello':id})


app.debug = DEBUG
app.run(host='0.0.0.0', port=PORT)