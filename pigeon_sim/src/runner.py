"""
Program to run the simulator based on the parameters provided by the user.

This program uses the `pigeon_sim` module to run the simulator as per the
Megha architecture and display/log the actions and results of the simulation.
"""

import os
import sys
import time
from typing_extensions import Final
import cProfile
import pstats
import io
from pstats import SortKey


from pigeon_sim import Simulation, simulator_globals
# from pigeon_sim import (SimulationLogger,
#                        debug_print,
#                        DEBUG_MODE)

from pigeon_sim import (debug_print,DEBUG_MODE)
if __name__ == "__main__":
    WORKLOAD_FILE: Final[str] = sys.argv[1]
    CONFIG_FILE: Final[str] = sys.argv[2]
    WORKLOAD_FILE_NAME: Final[str] = "subtrace_"+(os.path.basename(WORKLOAD_FILE)
                                      .split("_")[-1])

    NETWORK_DELAY = 0.005  # same as sparrow
    with open("logs/task_constraints.txt", "w"):
        pass 
    with open("logs/node_constraints.txt", "w"):
        pass
    with open("logs/JRT.txt","w"):
        pass
    with open("logs/tasks.txt","w"):
        pass

    # This is not the simulation's virtual time. This is just to
    # understand how long the program takes
    t1 = time.time()
    s = Simulation(WORKLOAD_FILE, CONFIG_FILE)
    
    print("Simulator Info , Simulation running")
    # logger.metadata("Simulator Info , Simulation running")
    
    if DEBUG_MODE:
        pr = cProfile.Profile()
        pr.enable()

    s.run()

    if DEBUG_MODE:
        pr.disable()
        text = io.StringIO()
        sortby = SortKey.CUMULATIVE
        ps = pstats.Stats(pr, stream=text).sort_stats(sortby)
        ps.print_stats()
        print(text.getvalue())

    time_elapsed = time.time() - t1
    print("Simulation ended in ", time_elapsed, " s ")
    print(f"Number of Jobs completed: {s.jobs_completed}")
    print("Simulator Info , Simulation ended")
    for master_id in s.masters:
        print(len(s.masters[master_id].high_priority_task_queue),len(s.masters[master_id].low_priority_task_queue))