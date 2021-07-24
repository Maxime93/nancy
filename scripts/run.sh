#!/bin/bash

ENV=$1

if [ -n "$ENV" ]; then
    echo "Running on $ENV"
    if [ $ENV == "raspberry" ]; then
        source /home/pi/nancy/nancy_venv/bin/activate
        python /home/pi/nancy/runner.py -e raspberry
    # else
        # Build the docker container before running
    fi
else
    echo "Empty Argument."
fi
