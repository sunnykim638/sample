#!/usr/bin/env bash
set -e
python3 user.py --name "$1" --manager-ip "$2" --manager-port "$3" --my-ip "$4" --mport "$5" --cport "$6"