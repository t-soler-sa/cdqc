#!/bin/bash

deptry . --no-ansi 2>&1 | grep "DEP002" | sed "s/.*DEP002 '\([^']*\)'.*/\1/" | sort -u > unused.txt

if [ -s unused.txt ]; then
    xargs pip uninstall -y < unused.txt
else
    echo "No unused packages found to uninstall."
fi

pip freeze > requirements_2.txt
pip install --upgrade -r requirements_2.txt