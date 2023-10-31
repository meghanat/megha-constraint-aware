"""
Program to run the simulator based on the parameters provided by the user.

This program uses the `megha_sim` module to run the simulator as per the
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


from megha_sim import Simulation, simulator_globals
from megha_sim import (SimulationLogger,
                       debug_print,
                       DEBUG_MODE)

if __name__ == "__main__":
    WORKLOAD_FILE: Final[str] = sys.argv[1]
    CONFIG_FILE: Final[str] = sys.argv[2]
    with open("logs/task_constraints.txt", "w"):
        pass 
    with open("logs/node_constraints.txt", "w"):
        pass 
    with open("logs/JRT.txt", "w"):
        pass
    with open("logs/tasks.txt", "w"):
        pass
    with open("logs/inconsistencies.txt", "w"):
        pass
    


    WORKLOAD_FILE_NAME: Final[str] = "subtrace_"+(os.path.basename(WORKLOAD_FILE)
                                      .split("_")[-1])

    logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()

    logger.metadata(f"Analysing logs for trace file: {WORKLOAD_FILE_NAME}")
    
    logger.metadata("Simulator Info , Received CMD line arguments.")

    NETWORK_DELAY = 0.005  # same as sparrow

    # This is not the simulation's virtual time. This is just to
    # understand how long the program takes
    t1 = time.time()
    s = Simulation(WORKLOAD_FILE, CONFIG_FILE)
    
    # print("Simulator Info , Simulation running")
    logger.metadata("Simulator Info , Simulation running")
    
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
    # logger.metadata(f"Simulation ended in {time_elapsed} s ")

    # debug_print(simulator_globals.jobs_completed)

    # print(f"Number of Jobs completed: {len(simulator_globals.jobs_completed)}")
    logger.metadata(
        "Simulator Info , Number of Jobs completed: "
        f"{len(simulator_globals.jobs_completed)}")

    # logger.integrity()
    # logger.flush()
    for GM_id in s.gms:
        if s.gms[GM_id].task_queue:
            for task_id in s.gms[GM_id].task_queue:
                print(s.gms[GM_id].task_queue[task_id])
        # print("Tasks completed:",GM_id,s.gms[GM_id].tasks_remaining)
        # print("Tasks arrived:",GM_id,s.gms[GM_id].total_tasks)
        # print("Tasks deleted:",GM_id,s.gms[GM_id].deleted_tasks)
        # print("Incomplete:",s.gms[GM_id].jobs_uncompleted)
        # print("jobs deleted:",s.gms[GM_id].deleted_jobs)
        # for job_id in s.gms[GM_id].jobs_uncompleted:
        #     print(type(s.gms[GM_id].jobs[job_id].tasks))
    # for LM_id in s.lms:
    #     print("Tasks Received:",LM_id,s.lms[LM_id].tasks_received)
    #     print("Tasks completed:",LM_id,s.lms[LM_id].tasks_completed)

    # print(simulator_globals.jobs_arrived)
    # print(simulator_globals.scheduling_attempts)
    print("Inconsistencies:",simulator_globals.inconsistencies)
    # print("Simulator Info , Simulation ended")
