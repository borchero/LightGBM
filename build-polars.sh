#!/bin/sh

pushd external_libs/polars
python -m pip install --upgrade pip
pip install --upgrade -r py-polars/requirements-dev.txt
pip install --upgrade -r py-polars/requirements-lint.txt
pip install --upgrade -r py-polars/docs/requirements-docs.txt
pip install --upgrade -r docs/requirements.txt

# enforce debug symbols
export RUSTFLAGS=-g

# slow binary with debug assertions and symbols, fast compile times
maturin develop -m py-polars/Cargo.toml | grep -v "don't match your environment"; test $${PIPESTATUS[0]} -eq 0
