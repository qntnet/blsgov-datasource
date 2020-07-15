#!/bin/sh

echo "Mode -> $1"

set -e

case "$1" in
 'update')
    while true; do
        python update.py -a
        sleep 3600
    done;
;;
 'server')
    gunicorn server:app
;;
esac
