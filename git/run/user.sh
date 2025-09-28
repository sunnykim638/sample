#!/usr/bin/env bash
set -e
python3 user.py --name "$1" --manager-ip "$2" --manager-port "$3" --mport "$4" --cport "$5"