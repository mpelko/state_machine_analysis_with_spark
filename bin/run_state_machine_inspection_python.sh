#!/usr/bin/env bash

#spark-submit --master local[4] lib/state_machine_inspection.py srv/data.txt
#spark-submit --master local[4] lib/state_machine_inspection.py ../raincoat/srv/data/timestamped_states_small.txt
spark-submit --master local[4] lib/state_machine_inspection.py srv/data_test.txt