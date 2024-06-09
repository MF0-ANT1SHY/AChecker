#!/bin/bash

if [ ! -d ".venv" ]; then
    echo "creating venv..."
    python3 -m venv .venv
    . .venv/bin/activate
    echo "activate venv success"
else
    echo "activating venv..."
    source .venv/bin/activate
    echo "activate venv success"
fi

which python