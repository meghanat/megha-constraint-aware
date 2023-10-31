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
from simulation_logger import SimulationLogger
import simulator_utils.globals
from simulator_utils.values import (LM_HEARTBEAT_INTERVAL, NETWORK_DELAY,
                                    InconsistencyType,
                                    TaskDurationDistributions)
import os
import sys
WORKLOAD_FILE= sys.argv[1]

WORKLOAD_FILE_NAME = "subtrace_"+(os.path.basename(WORKLOAD_FILE)
                                      .split("_")[-1])

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from local_master import LM
    from global_master import GM
    from simulation import Simulation

# Get the logger object for this module
logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()


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

    def __init__(self, task: Task):
        """
        Initialise the instance of the `TaskEndEvent` class.

        Args:
                task (Task): The task object representing the task which has \
                completed.
        """
        self.task: Task = task

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
        # print(current_time,"TaskEndEvent","job:",self.task.job.job_id,"task:",self.task.task_id,"node:",self.task.node_id,"LM:",self.task.lm.LM_id,"GM:",self.task.GM_id, "partition_id:",self.task.partition_id)
        # Log the TaskEndEvent
        logger.info(f"{current_time} , "
                    "TaskEndEvent , "
                    f"{self.task.job.job_id} , "
                    f"{self.task.task_id} , "
                    f"{self.task.duration}")
        self.task.end_time = current_time
        if self.task.lm is not None:
            self.task.lm.task_completed(self.task)

###############################################################################
###############################################################################


class LaunchOnNodeEvent(Event):
    """
    Event created after the Local Master verifies the Global Master's request.

    This event is created when a task is sent to a particular worker node in a
    particular partition, selected by the global master and verified by the
    local master.

    Args:
            Event (Event): Parent Event class.
    """

    def __init__(self, task: Task, simulation: Simulation):
        """
        Initialise the instance of the `LaunchOnNodeEvent` class.

        Args:
            task (Task): The Task object to be launched on the selected node.
            simulation (Simulation): The simulation object to insert events \
            into.
        """
        self.task = task
        self.simulation = simulation

    def run(self, current_time: float):
        """
        Run the actions to handle a `LaunchOnNodeEvent`.

        Run the actions to perform on the event of launching a task on the \
        node.

        Args:
            current_time (float): The current time in the simulation.
        """

        assert self.task.node_id is not None
        # Log the LaunchOnNodeEvent
        logger.info(
            f"{current_time} , "
            "LaunchOnNodeEvent , "
            f"{self.task.job.job_id} , "
            f"{self.task.task_id} , "
            f"{self.task.partition_id} , "
            f"{self.task.node_id} , "
            f"{self.task.duration} , "
            f"{self.task.job.start_time}")

        # print(current_time,"LaunchOnNodeEvent","job:",self.task.job.job_id,"task:",self.task.task_id,"node:",self.task.node_id,"LM:",self.task.lm.LM_id,"GM:",self.task.GM_id)

        # launching requires network transfer
        self.simulation.event_queue.put(
            (current_time + self.task.duration, TaskEndEvent(self.task)))


##########################################################################
##########################################################################


class InconsistencyEvent(Event):
    """
    Event created when the Global Master tries placing a task on a busy node.

    This happens when the Global Master has outdated information about the
    cluster. This event is created by the Local Master.

    Args:
        Event (Event): Parent Event class.
    """

    def __init__(self, task: Task, gm: GM,lm: LM,status,
                 simulation: Simulation):
        """
        Initialise the instance of the InconsistencyEvent class.

        Args:
            task (Task): The Task object that caused the inconsistency.
            gm (GM): The Global Master responsible for the inconsistency.
            type (InconsistencyType): The type of inconsistency caused.
            simulation (Simulation): The simulation object to insert events \
            into.
        """
        self.task: Task = task
        self.gm: GM = gm
        self.simulation: Simulation = simulation
        self.status=status
        self.lm=lm

    def run(self, current_time: float):
        """
        Run the actions to handle the `InconsistencyEvent`.

        Run the actions to perform on the event of the Global
        Master requesting to place a task on a busy worker node.

        Args:
            current_time (float): The current time in the simulation.
        """
        task_id: str = self.task.task_id
        job_id: str = self.task.job.job_id
        job_id_task_id = f"{job_id}_{task_id}"
        

        # If the job is already moved to jobs_scheduled queue, then we need to
        # remove it and add it to the front of the queue.
        self.gm.unschedule_task(self.task)
        self.gm.update_status(self.lm,self.status,current_time)
        
##########################################################################
##########################################################################


class BatchedInconsistencyEvent(Event):
    """
    Event created when the Global Master tries placing a task on a busy node.

    This happens when the Global Master has outdated information about the
    cluster. This event is created by the Local Master.

    Args:
        Event (Event): Parent Event class.
    """

    def __init__(self, inconsistent_task_mappings: List, gm: GM, lm: LM,
                 simulation: Simulation, lm_update):
        """
        Initialise the instance of the InconsistencyEvent class.

        Args:
            task (Task): The Task object that caused the inconsistency.
            gm (GM): The Global Master responsible for the inconsistency.
            type (InconsistencyType): The type of inconsistency caused.
            simulation (Simulation): The simulation object to insert events \
            into.
        """
        self.inconsistent_task_mappings= inconsistent_task_mappings
        self.gm: GM = gm
        
        self.simulation: Simulation = simulation
        self.lm_update=lm_update
        self.lm=lm

    def run(self, current_time: float):
        """
        Run the actions to handle the `InconsistencyEvent`.

        Run the actions to perform on the event of the Global
        Master requesting to place a task on a busy worker node.

        Args:
            current_time (float): The current time in the simulation.
        """
        # If the job is already moved to jobs_scheduled queue, then we need to
        # remove it and add it to the front of the queue.

        for task_mapping in self.inconsistent_task_mappings:
            self.gm.unschedule_task(task_mapping["task"])

        self.gm.update_status(self.lm,self.lm_update,current_time)
        

##########################################################################
##########################################################################
# created when GM finds a match in the external or internal partition
class VerifyRequestsEvent(Event):
    """
    Event created when the Global Master finds a free worker node.

    Args:
        Event (Event): Parent Event class.
    """

    def __init__(
            self,
            task_mappings: List,
            gm: GM,
            lm: LM
            ):
        """
        Initialise the instance of the `VerifyRequestEvent` class.

        Args:
            task (Task): The task object to launch on the selected node.
            gm (GM): The Global Master that wants to allocate the task to \
            a worker node.
            lm (LM): The Local Master, the selected worker node belongs to.
            node_id (str): The identifier of the selected worker node.
            current_time (float): The current time in the simulation.
            external_partition (Optional[str], optional): Identifier of \
            the Global Master to which the external partition (if selected) \
            belongs to. Defaults to None.
        """
        self.task_mappings = task_mappings
        self.gm = gm
        self.lm = lm
        


    def run(self, current_time: float):
        """
        Run the actions to handle the `VerifyRequestEvent`.

        Request the Local Master to which the worker node belongs, to
        verify if the worker node is indeed free to take up a task.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the VerifyRequestEvent
        # logger.info(f"{current_time} , "
        #             "VerifyRequestsEvent , "
        #             f"{self.task.job.job_id}_{self.task.task_id} , "
        #             f"{self.gm.GM_id}_{self.lm.LM_id}_{self.node_id}")
        
        
        self.lm.verify_requests(
            self.task_mappings,
            self.gm,
            current_time)

##########################################################################
##########################################################################

# created when GM finds a match in the external or internal partition
class VerifyRequestEvent(Event):
    """
    Event created when the Global Master finds a free worker node.

    Args:
        Event (Event): Parent Event class.
    """

    def __init__(
            self,
            task: Task,
            gm: GM,
            lm: LM):
        """
        Initialise the instance of the `VerifyRequestEvent` class.

        Args:
            task (Task): The task object to launch on the selected node.
            gm (GM): The Global Master that wants to allocate the task to \
            a worker node.
            lm (LM): The Local Master, the selected worker node belongs to.
            node_id (str): The identifier of the selected worker node.
            current_time (float): The current time in the simulation.
            external_partition (Optional[str], optional): Identifier of \
            the Global Master to which the external partition (if selected) \
            belongs to. Defaults to None.
        """
        self.task = task
        self.gm = gm
        self.lm = lm


    def run(self, current_time: float):
        """
        Run the actions to handle the `VerifyRequestEvent`.

        Request the Local Master to which the worker node belongs, to
        verify if the worker node is indeed free to take up a task.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the VerifyRequestEvent
        logger.info(f"{current_time} , "
                    "VerifyRequestEvent , "
                    f"{self.task.job.job_id}_{self.task.task_id} , "
                    f"{self.gm.GM_id}_{self.lm.LM_id}_{self.task.node_id}")
        # print(current_time,"verifying request",self.task.job.job_id,self.task.task_id,self.lm.LM_id,self.node_id)
        
        self.lm.verify_request(
            self.task,
            self.gm,
            current_time,
            )

##########################################################################
##########################################################################
# created periodically or when LM needs to piggyback update on response
class TaskResponseEvent(Event):
    """
    Event created when task responds to its GM syaing it has completed
    execution and that it has released resources
    """

    def __init__(self,simulation: Simulation,gm: GM,task: Task):
        self.simulation=simulation
        self.gm=gm
        self.task=task

    def run(self, current_time: float):
        self.gm.receive_task_response(current_time,self.task)


class LMStatusUpdateEvent(Event):
    """
    Event created when Local Masters send updates to Global Masters

    Args:
        Event (Event): Parent `Event` class.
    """

    def __init__(self,status,simulation: Simulation,lm):
        """
        Initialise the instance of the `LMRequestUpdateEvent` class.

        Args:
            simulation (Simulation): The simulation object to insert events \
            into.
            periodic (bool, optional): Whether the updates are periodic or not\
            . Defaults to True.
            gm (Optional[GM], optional): The Global Master that needs to be \
            updated about the state of the cluster. Defaults to None.
        """
        self.simulation: Simulation = simulation
        self.lm=lm
        self.status=status
        
    def run(self, current_time: float):
        for GM_id in self.simulation.gms:
            # print(current_time,GM_id,"callin lm status update")
            self.simulation.gms[GM_id].update_status(self.lm,self.status[GM_id],current_time)
        
##########################################################################
##########################################################################


class LMRequestUpdateEvent(Event):
    """
    Event created when LM Heartbeat is to be sent to all Global Masters periodically

    Args:
        Event (Event): Parent `Event` class.
    """

    def __init__(self, simulation: Simulation):
        """
        Initialise the instance of the `LMRequestUpdateEvent` class.

        Args:
            simulation (Simulation): The simulation object to insert events \
            into.
        """
        self.simulation: Simulation = simulation
        self.first_update=True

    def run(self, current_time: float):
        """
        Run the actions to handle the `LMRequestUpdateEvent`.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the LMRequestUpdateEvent
        logger.info(f"{current_time} , LMRequestUpdateEvent")

        
        are_jobs_done = True
        for GM_id in self.simulation.gms:
            # if len(self.simulation.gms[GM_id].task_queue) > 0:
            if self.simulation.gms[GM_id].task_queue or self.first_update:
                are_jobs_done = False
                self.first_update=False
                # print("Tasks pending:",len(self.simulation.gms[GM_id].task_queue))
                # print(GM_id,"Tasks pending:")
                # for task in self.simulation.gms[GM_id].task_queue:
                #     print("Task not done:",task.job.job_id,task.task_id,task.constraints)
                # print(GM_id,"Resources available:",self.simulation.gms[GM_id].internal_available_nodes)
                break

        if not are_jobs_done or not self.simulation.event_queue.empty():
            for LM_id in self.simulation.lms:
                self.simulation.lms[LM_id].send_status_update(
                    current_time)

            self.simulation.event_queue.put(
                (current_time + LM_HEARTBEAT_INTERVAL,
                 self))#add the next request

##########################################################################
##########################################################################
# created for each job


class JobArrivalEvent(Event):
    """
    Event created on the arrival of a `Job` into the user queue.

    Args:
        Event (Event): Parent `Event` class.
    """

    gm_counter: int = 0

    def __init__(self, simulation: Simulation,
                 task_distribution: TaskDurationDistributions,
                 job: Job,
                 jobs_file: TextIOWrapper):
        """
        Initialise the instance of the `JobArrival` class.

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

    def __lt__(self, other: Event) -> bool:
        """
        Compare the `JobArrival` object with another object of Event class.

        Args:
            other (Event): The object to compare with.

        Returns:
            bool: The `JobArrival` object is always lesser than the object \
                  it is compared with.
        """
        return True

    def run(self, current_time: float):
        """
        Run the actions to handle the `JobArrival` event.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the JobArrival
        # logger.info(f"{current_time} , JobArrival , {self.task_distribution}")

        new_events: List[Tuple[float, Event]] = []
        # needs to be assigned to a GM - RR
        
        JobArrivalEvent.gm_counter = (JobArrivalEvent.gm_counter%
                                 self.simulation.NUM_GMS) + 1

        # assigned_GM --> Handle to the global master object
        assigned_GM: GM = self.simulation.gms[str(JobArrivalEvent.gm_counter)]
        # GM needs to add job to its queue
        assigned_GM.schedule_job_batched_all(self.job, current_time)

        # Creating a new Job Arrival event for the next job in the trace
        line = self.jobs_file.readline()
        if len(line) == 0:
            self.simulation.scheduled_last_job = True
        else:
            self.job = Job(self.task_distribution, line, self.simulation)
            new_events.append((self.job.start_time, self))
            self.simulation.jobs_scheduled += 1
        return new_events
