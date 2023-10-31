"""The `Task` class is just like a struct or Plain Old Data format."""
from __future__ import annotations
from typing import Optional, TYPE_CHECKING
import random

if TYPE_CHECKING:
    from job import Job

seed_value = 42
random.seed(seed_value)

class Task(object):
    seed_value = 42
    rand_obj=random.Random()
    rand_obj.seed(seed_value)

    """
    The Task class is just like a struct in languages such as C.

    This is otherwise known as the Plain Old Data format.

    Args:
        object (object): This is the parent object class
    """

    def __init__(self, task_id: str, job: Job, duration: int):
        """
        Initialise the instance of the Task class.

        Args:
            task_id (str): The task identifier.
            job (Job): The instance of the Job to which the Task belongs.
            duration (int): Duration of the task.
        """
        
        self.task_id = task_id
        self.start_time = job.start_time
        self.scheduled_time = None
        self.end_time: Optional[float] = None
        self.job = job
        self.duration = duration
        self.node_id: Optional[str] = None
        self.scheduling_attempts=0
        self.communication_delay=0
        self.repartitions=0
        self.constraints=self.assign_task_constraints()
        self.no_match=set()
        # Partition ID may differ from GM_id if repartitioning
        self.partition_id: None
        self.GM_id = None
        self.lm = None
        self.scheduled = False
        with open("logs/task_constraints.txt", "a") as file:
            file.write(str(self.start_time)+",")
            file.write(self.job.job_id+","+self.task_id+",")
            file.write(",".join(str(constraint) for constraint in self.constraints))
            file.write("\n")
        

    def assign_task_constraints(self):
        constraints=[]
        task_type=Task.rand_obj.choices(["1","2","3","4"],k=1)[0]
        weights=self.job.simulation.task_occurrence_type[task_type]
        statistical_cluster=Task.rand_obj.choices(["1","2","3","4","5","6","7","8","9","10"],weights=weights,k=1)[0]
        for j in range(0,len(self.job.simulation.tcfv[statistical_cluster])):
            if(Task.rand_obj.random()*100<=self.job.simulation.tcfv[statistical_cluster][j]):
                constraints.append(j)
        # print(self.job.job_id,self.task_id,constraints)
        return constraints     

    def __str__(self):
        return "job:"+str(self.job.job_id)+" task:"+str(self.task_id)+" constraints:"+str(self.constraints)

