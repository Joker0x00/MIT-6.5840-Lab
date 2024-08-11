#!/bin/bash
for i in {0..0}; 
do 
    go test -run TestPersist23C > "${i}.log"
    if grep -q "Passed" "${i}.log"; then
        echo "${i} Passed"
    fi
done
# clear
# python3 parse.py ./res/TestFigure8Unreliable3C_8.log -c 5 