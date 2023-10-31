"""
File containing the implementation of the Global master.

The file contains the implementation of the Global master module of the \
modified scheduler architecture.
"""

from __future__ import annotations
import json
import random
from typing import List, Dict, TYPE_CHECKING
from bitstring import BitArray
from events import  TaskArrivedAtMasterEvent

import simulator_utils.globals
from simulator_utils import debug_print
from simulator_utils.values import (NETWORK_DELAY,
									TaskDurationDistributions)
# from simulation_logger import (SimulationLogger, MATCHING_LOGIC_MSG,
# 							   CLUSTER_SATURATED_MSG,
# 							   MATCHING_LOGIC_REPARTITION_MSG)
from watchpoints import watch
# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
	from job import Job
	from master import Master

# Seed the random number generator
# random.seed(47)

import os
import sys
WORKLOAD_FILE: Final[str] = sys.argv[1]

WORKLOAD_FILE_NAME: Final[str] = (os.path.basename(WORKLOAD_FILE)
									  .split(".")[0])

# logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()


class Distributor:
	# Starting value for the seeds for the Random objects
	SEED_VALUE = 13

	def __init__(self, simulation, distributor_id: str, WORKERS_CONSTRAINT_SETS):
		self.distributor_id = distributor_id
		self.simulation = simulation
		self.RR_counter: int = 0
		
		self.jobs_scheduled=dict()
		self.random_obj = random.Random()
		self.random_obj.seed(Distributor.SEED_VALUE)
		Distributor.SEED_VALUE += 13

		# maintain per constraint demand per master

		#initialize all worker constraints
		self.WORKERS_CONSTRAINT_SETS=WORKERS_CONSTRAINT_SETS
		debug_print(f"Distributor {self.distributor_id} initialised")

	def schedule_job(self,job,current_time):
		# print(self.distributor_id,current_time, ",", "JobArrivalAtDistributor",",", job.job_id)
		#distribute tasks based on demand for constraint
		# and if constraint can be satisfied by any single worker in subcluster
		job.ideal_completion_time=0
		for task in job.tasks:
			# print("Job:",job.job_id,"Task:",task.task_id)
			constraint_occurrence=dict()
			constraints=set(task.constraints)
			flag=False

			for master_id in self.simulation.masters:
				constraint_occurrence[master_id]=0
				for constraint_set in self.WORKERS_CONSTRAINT_SETS[master_id]:
					if set(task.constraints).issubset(constraint_set):
						value=self.WORKERS_CONSTRAINT_SETS[master_id][constraint_set]
						constraint_occurrence[master_id]+=value
						if(value>0):
							flag=True
			if not flag:
				# print(job.job_id,task.task_id,"NO WORKER SATISFYING CONSTRAINT")
				job.num_tasks-=1
				if(job.num_tasks==0):
					print("No job:",job.job_id)
			else:
				if(task.duration>job.ideal_completion_time):
					job.ideal_completion_time=task.duration
				master_id=random.choices(list(constraint_occurrence.keys()),weights=list(constraint_occurrence.values()),k=1)[0]
				if(constraint_occurrence[master_id]==0):
					print("ERROR")
					exit()
			
				task.master=self.simulation.masters[master_id]
				task.distributor=self
				# print(self.distributor_id,"Distributing:",task,"to",master_id)
				self.simulation.event_queue.put((current_time+NETWORK_DELAY, TaskArrivedAtMasterEvent(self.simulation,task)))
				self.jobs_scheduled[job.job_id]=job

	def task_completion(self,task,current_time):
		# print(self.distributor_id,current_time,"TCD:",task,current_time-task.start_time-task.duration)
		job=self.jobs_scheduled[task.job.job_id]
		job.completed_tasks.append(task)
		with open("logs/tasks.txt","a") as file:
			file.write(job.job_id+","+task.task_id+","+str(task.start_time))
			file.write(","+str(current_time)+"\n")
		if len(job.completed_tasks)==job.num_tasks:#job completed
			with open("logs/JRT.txt", "a") as file:
				file.write(str(current_time)+",")
				file.write(job.job_id+",")
				file.write(str(current_time-job.start_time-job.ideal_completion_time))
				file.write("\n")
			print("JC",current_time,",",job.job_id,",",current_time-job.start_time-job.ideal_completion_time)
			self.simulation.jobs_completed+=1
		return
