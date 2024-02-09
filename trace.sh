#!/bin/bash

PYTHONMALLOC=malloc python -m memray run --native --output output.bin test.py

python -m memray flamegraph output.bin -o flamegraph.html
python -m memray flamegraph --inverted output.bin -o flamegraph--inverted.html
# python -m memray flamegraph --leak output.bin -o flamegraph--leak.html
# python -m memray flamegraph --leak --inverted output.bin -o flamegraph--leak--inverted.html