sbt package && spark-submit --class "StateMachineInspection" --master local[4] target/scala-2.11/state_machine_analysis_2.11-1.0.jar srv/data.txt
