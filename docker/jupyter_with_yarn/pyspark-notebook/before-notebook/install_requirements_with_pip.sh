#!/usr/bin/env bash
REQFILE="/home/jovyan/work/requirements_pyspark_notebook.txt"
echo $REQFILE
if [ -f "$REQFILE" ]; then
    pip install -r "$REQFILE"
fi
