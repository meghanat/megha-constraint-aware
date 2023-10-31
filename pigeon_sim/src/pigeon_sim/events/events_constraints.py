"""
This file contains the implementation of various `Events` in the simulator.

Raises:
    NotImplementedError: This exception is raised when attempting to create \
    an instance of the `Event` class.
    NotImplementedError: This exception is raised when attempting to call \
    `run` on an instance of the `Event` class.
"""

from __future__ import annotations
from io import TextIOWrapper
from typing import List, Optional, Tuple, TYPE_CHECKING
import sys
from job import Job
from task import Task
# from simulation_logger import SimulationLogger
from simulator_utils.values import ( NETWORK_DELAY,
                                    TaskDurationDistributions)
import os
import sys
WORKLOAD_FILE= sys.argv[1]

WORKLOAD_FILE_NAME = "subtrace_"+(os.path.basename(WORKLOAD_FILE)
                                      .split("_")[-1])

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from master import Master
    from distributor import Distributor
    from simulation import Simulation

# Get the logger object for this module
# logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()


class Event(object):
    """
    This is the abstract `Event` object class.

    Args:
        object (Object): Parent object class.
    """

    def __init__(self):
        """
        One cannot initialise the object of the abstract class `Event`.

        This raises a `NotImplementedError` on attempting to create \
        an object of the class.

        Raises:
            NotImplementedError: This exception is raised when attempting to \
            create an instance of the `Event` class.
        """
        raise NotImplementedError(
            "Event is an abstract class and cannot be instantiated directly")

    def __lt__(self, other: Event) -> bool:
        """
        Compare the `Event` object with another object of `Event` class.

        Args:
            other (Event): The object to compare with.

        Returns:
            bool: The Event object is always lesser than the object it is \
            compared with.
        """
        return True

    def run(self, current_time: float):
        """
        Run the actions to handle the `Event`.

        Args:
            current_time (float): The current time in the simulation.

        Raises:
            NotImplementedError: This exception is raised when attempting to \
            call `run` on an instance of the `Event` class.
        """
        # Return any events that should be added to the queue.
        raise NotImplementedError(
            "The run() method must be implemented by each class subclassing "
            "Event")


##########################################################################
##########################################################################


class TaskEndEvent(Event):
    """
    This event is created when a task has completed.

    The `end_time` is set as the `current_time` of running the event.

    Args:
            Event (Event): Parent Event class.
    """

    def __init__(self,simulation, task: Task):
        """
        Initialise the instance of the `TaskEndEvent` class.

        Args:
                task (Task): The task object representing the task which has \
                completed.
        """
        self.task: Task = task
        self.simulation=simulation

    def __lt__(self, other: Event) -> bool:
        """
        Compare the `TaskEndEvent` object with another object of Event class.

        Args:
                other (Event): The object to compare with.

        Returns:
                bool: The `TaskEndEvent` object is always lesser than the \
                object it is compared with.
        """
        return True

    def run(self, current_time: float):
        """
        Run the actions to perform on the event of task completion.

        Args:
            current_time (float): The current time in the simulation.
        """
        # print(current_time,"TaskEndEvent","job:",self.task.job.job_id,"task:",self.task.task_id,"Master:",self.task.master.master_id,"Distributor:",self.task.distributor_id)
        # Log the TaskEndEvent
        # logger.info(f"{current_time} , "
        #             "TaskEndEvent , "
        #             f"{self.task.job.job_id} , "
        #             f"{self.task.task_id} , "
        #             f"{self.task.duration}")
        self.task.end_time = current_time
        
        self.task.master.idle_worker_notice(self.task,current_time)
        self.task.distributor.task_completion(self.task,current_time)

###############################################################################
###############################################################################
class TaskArrivedAtWorkerEvent(Event):
    """
    Event created after master sends task to worker
    """

    def __init__(self,simulation,task):
        self.simulation=simulation
        self.task=task

    def run(self,current_time):
        # print(current_time,"TaskArrivedAtWorkerEvent, ",self.task.job.job_id,"/", self.task.task_id)
        self.simulation.event_queue.put((current_time+self.task.duration+NETWORK_DELAY,TaskEndEvent(self.simulation,self.task)))

##########################################################################
##########################################################################
# created for each job
class TaskArrivedAtMasterEvent(Event):
    """
    Event created after distributor sends task to master
    """

    def __init__(self,simulation,task):
        self.simulation=simulation
        self.task=task

    def run(self,current_time):
        # print(current_time,"TaskArrivedAtMasterEvent, ",self.task.job.job_id,"/", self.task.task_id, self.task.master.master_id)
        self.task.master.schedule_task(self.task,current_time)
        

class JobArrivalEvent(Event):
    """
    Event created on the arrival of a `Job` into the user queue.

    Args:
        Event (Event): Parent `Event` class.
    """

    distributor_counter: int = 0


    def __init__(self, simulation: Simulation,
                 task_distribution: TaskDurationDistributions,
                 job: Job,
                 jobs_file: TextIOWrapper):
        """
        Initialise the instance of the `JobArrivalEvent` class.

        Args:
            simulation (Simulation): The simulation object to insert events \
            into.
            task_distribution (TaskDurationDistributions): Select the \
            distribution of the duration/run-time of the tasks of the Job
            job (Job): The Job object that has arrived into the user queue.
            jobs_file (TextIOWrapper): File handle to the input trace file.
        """
        self.simulation = simulation
        self.task_distribution = task_distribution
        self.job = job
        self.jobs_file = jobs_file  # Jobs file (input trace file) handler

    # def __lt__(self, other: Event) -> bool:
    #     """
    #     Compare the `JobArrivalEvent` object with another object of Event class.

    #     Args:
    #         other (Event): The object to compare with.

    #     Returns:
    #         bool: The `JobArrivalEvent` object is always lesser than the object \
    #               it is compared with.
    #     """
    #     return True

    def run(self, current_time: float):
        """
        Run the actions to handle the `JobArrivalEvent` event.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the JobArrivalEvent
        # logger.info(f"{current_time} , JobArrivalEvent , {self.task_distribution}")
        # print(current_time, ",", "JobArrivalEvent",",", self.job.job_id)

        #assign job to a distributor at random

        self.assigned_Distributor=self.simulation.distributors[str(self.simulation.rand_obj.randint(1,self.simulation.NUM_distributors))]
        self.assigned_Distributor.schedule_job(self.job,current_time)
        
        # Creating a new Job Arrival event for the next job in the trace
        line = self.jobs_file.readline()
        if len(line) == 0:
            self.simulation.scheduled_last_job = True
        else:
            self.job = Job(self.task_distribution, line, self.simulation)
            self.simulation.event_queue.put(
                    (self.job.start_time,
                     self))
            self.simulation.jobs_scheduled += 1
    