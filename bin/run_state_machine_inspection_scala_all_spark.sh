#!/usr/bin/env bash

#sbt package && spark-submit --class "SeriesReduction.SeriesReduction" --master=local[4] target/scala-2.11/state_machine_analysis_2.11-1.0.jar srv/data.txt
#sbt package && spark-submit --class "SeriesReduction.SeriesReduction" --master=local[4] target/scala-2.11/state_machine_analysis_2.11-1.0.jar srv/data_test.txt

sbt package && spark-submit --class "SeriesReduction.SeriesReduction" --master local[4] target/scala-2.11/state_machine_analysis_2.11-1.0.jar ../raincoat/srv/data/timestamped_states_medium.txt
