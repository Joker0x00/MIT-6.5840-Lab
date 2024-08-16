#!/bin/bash
# for i in {0..0}; 
# do 
#     go test -run TestBasicAgree3B > "${i}.log"
#     if grep -q "Passed" "${i}.log"; then
#         echo "${i} Passed"
#     fi
# done
clear
python3 parse.py 0.log -c 3 