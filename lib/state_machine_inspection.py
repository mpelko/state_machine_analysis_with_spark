"""
state_machine_inspection.py

A minimal conceptual example of how we extract intervals 
(=timings of state changes) from a state machine defined 
by log messages.
"""
import sys
from pyspark import SparkContext

def clean_up(candidate_changes):
    changes = []
    status = 0
    for i, candidate in enumerate(candidate_changes):
        if (i == 0) or (status != candidate[1]):
            status = candidate[1]
            changes.append(candidate)
    return changes


def map_relevant(iterat):
    all = list(iterat)
    first = all[0]
    status = first[1]
    yield first
    for record in all:
        if status != record[1]:
            status = record[1]
            yield record
    yield all[-1]

logFile = sys.argv[1]
sc = SparkContext("local[4]", "State Machine Inspection")
logData = sc.textFile(logFile, 4)
partial_result = (logData.map(lambda x: x.split(" "))
                         .map(lambda x: (float(x[0][:-1]), int(x[1])))
                         .mapPartitions(map_relevant)
                         .collect()
                  )

changes = clean_up(partial_result)
print("\nLast 20 transitions:")
print(changes[-20:])
print("\nNumber of transitions found:")
print(len(changes))
