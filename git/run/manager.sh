#!/usr/bin/env bash
set -e
python3 manager/manager.py --host 0.0.0.0 --mport "$1"
