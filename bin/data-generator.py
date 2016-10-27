#!/usr/bin/env python

"""
Generates a mock log consisting of comma separated timestamp (float, equidistant from line to line) and status (0 or 1).
"""

from __future__ import print_function
import random
import argparse
import sys

def state_generator(state_change_probability, dt):
    """
    A generator returning tuples of timestamp and status.
    """
    ts, state= 0, 0
    while True:
        if random.random() < state_change_probability:
            state = (state + 1) % 2 
        ts += dt
        yield ts, state


def main(args):
    state_gen = state_generator(args.state_change_probability, 
                                args.dt)
    for _ in range(args.N):
        print("{:.3f}, {}".format(*next(state_gen)), file=args.outfile)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-N', help="Number of log records", type=int, default=1000000)
    parser.add_argument('-p', '--state_change_probability', help="Probability of the state change between two records.", 
                        type=float, default=0.001)
    parser.add_argument('-d', '--dt', help="The increment of the timestamp between two records", 
                        type=float, default=0.001)
    parser.add_argument('-o', '--outfile', help="Output file",
                        default=sys.stdout, type=argparse.FileType('w'))

    args = parser.parse_args(sys.argv[1:])

    main(args)

