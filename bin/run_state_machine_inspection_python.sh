#!/usr/bin/env bash

spark-submit --master local[4] lib/state_machine_inspection.py srv/data.txt
