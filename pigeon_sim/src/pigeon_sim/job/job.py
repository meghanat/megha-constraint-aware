
"""
This file contains the implementation of the Job class.

The Job class is used to describe a job that is input to the scheduler.
A job consists of one or more tasks. The instance of the Job class keeps
track of various parameters related to task assignment and completion.
"""

from typing import Dict, List, Optional, TYPE_CHECKING
from typing_extensions import Final

from task import Task
from simulator_utils.values import TaskDurationDistributions
from simulator_utils.values import (Master_HEARTBEAT_INTERVAL, NETWORK_DELAY,
                                    InconsistencyType,
                                    TaskDurationDistributions,TASK_FILE)

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from distributor import Distributor


class Job(object):
    job_count = 1  # To assign IDs
    job_start_tstamps: Dict[float, float] = {}

    def __init__(self, task_distribution: TaskDurationDistributions, line: str,
                 simulation):
        """
        Retaining below logic as-is to compare with Sparrow.

        During initialisation of the object, we dephase the incoming job in
        case it has the exact submission time as another already submitted job

        Args:
            task_distribution (TaskDurationDistributions): In case we need to
            explore other task distribution methods. This is retained from the
            Sparrow code as-is.
            line (str): Line from the input trace file
            simulation (Simulation): The object of the simulation class. This
            is not currently begin used in the class's internal implementation.
        """

        job_args: List[str] = line.strip().split()
        self.start_time: float = float(job_args[0])
        self.num_tasks: int = int(job_args[1])
        self.simulation = simulation
        self.tasks: List = []
        self.task_counter = 0
        self.completed_tasks = []
        self.distributor: Optional[Distributor] = None
        self.completion_time: float = -1.
        self.is_high_priority=False
        if float(job_args[2])<self.simulation.cutoff:
            # print("HP")
            self.is_high_priority=True
        # self.stat_file=open(TASK_FILE,"a")
        self.vals = [int(float(i)) for i in job_args[3:]]
        self.ideal_completion_time: Final = max(self.vals)

        # IF the job's start_time has never been seen before
        if self.start_time not in self.job_start_tstamps:
            # Add it to the dict of start time stamps
            self.job_start_tstamps[self.start_time] = self.start_time
        else:  # If the job's start_time has been seen before
            # Shift the start time of the jobs with this duplicate start time by 0.01s forward to prevent
            # a clash
            self.job_start_tstamps[self.start_time] += 0.01
            # Assign this shifted time stamp to the job start time
            self.start_time = self.job_start_tstamps[self.start_time]

        self.job_id = str(Job.job_count)
        Job.job_count += 1

        self.end_time = self.start_time

        # in case we need to explore other distr- retaining Sparrow code as-is
        if task_distribution == TaskDurationDistributions.FROM_FILE:
            self.file_task_execution_time(job_args)

    
    # Job class - parse file line

    def file_task_execution_time(self, job_args):
        # Adding each of the tasks to the dict
        for task_duration in (job_args[3:]):
            # Same as eagle_simulation.py, This is done to read the floating
            # point value from the string and then convert it to an int
            duration = int(float(task_duration))
            self.tasks.append(Task(str(self.task_counter), self, duration))
            self.task_counter += 1
