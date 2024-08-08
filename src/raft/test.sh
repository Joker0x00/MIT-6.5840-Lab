#!/bin/bash
time go test -run 3B > 1.log
# clear
# python3 parse.py 1.log -c 3