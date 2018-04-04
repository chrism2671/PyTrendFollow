#!/bin/sh
./download.py quandl --concurrent
./download.py ib
./validate.py
