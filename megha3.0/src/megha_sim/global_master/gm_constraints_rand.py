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

import simulator_utils.globals
from simulator_utils import debug_print
from simulator_utils.values import (LM_HEARTBEAT_INTERVAL, NETWORK_DELAY,
									InconsistencyType,
									TaskDurationDistributions)
from events import VerifyRequestEvent,VerifyRequestsEvent
from simulation_logger import (SimulationLogger, MATCHING_LOGIC_MSG,
							   CLUSTER_SATURATED_MSG,
							   MATCHING_LOGIC_REPARTITION_MSG)
from .gm_types import (PartitionKey, LMResources, ConfigFile,
					   OrganizedPartitionResources, NodeResources,
					   PartitionResources)
from watchpoints import watch
# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
	from job import Job
	from local_master import LM

# Seed the random number generator
random.seed(43)

import os
import sys
WORKLOAD_FILE: Final[str] = sys.argv[1]

WORKLOAD_FILE_NAME: Final[str] = (os.path.basename(WORKLOAD_FILE)
									  .split(".")[0])

logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()


class GM:
	"""
	Class defining the implementation of the Global Master.

	This class provides the implementation of the different \
	interfaces provided by the Global Master to the rest of \
	the scheduler architecture.
	"""
	jobs_completed=0
	# Starting value for the seeds for the Random objects
	SEED_VALUE = 42

	def __init__(self, simulation, GM_id: str, config: ConfigFile,WORKER_CONSTRAINTS_SET):
		self.GM_id = GM_id
		self.jobs={}
		self.simulation = simulation
		self.task_queue: List[Job] = []
		self.random_obj = random.Random()
		self.random_obj.seed(GM.SEED_VALUE)
		self.PARTITON_SIZE=len(config["LMs"]["1"]["partitions"]["1"][0])-2 #to get rid of 0b
		self.WORKER_CONSTRAINTS_VECTOR=dict()#does not change [LM_id][partition_id][constraint_index]
		self.WORKER_CONSTRAINTS=dict()
		self.WORKER_CONSTRAINTS_SET=WORKER_CONSTRAINTS_SET
		GM.SEED_VALUE += 13
		
		self.internal_available_nodes = dict()
		self.external_available_nodes = dict()
		self.external_busy_partitions=set()
		self.internal_busy_partitions=set()
		
		self.LMs_list=list(config["LMs"].keys())
		self.GMs_list=list(config["LMs"]["1"]["partitions"].keys())
		for LM_id in config["LMs"]:
			
			self.external_available_nodes[LM_id]=dict()
			self.WORKER_CONSTRAINTS_VECTOR[LM_id]=dict()
			for partition_id in config["LMs"][LM_id]["partitions"]:
				self.WORKER_CONSTRAINTS_VECTOR[LM_id][partition_id]=list()
				for constraint_vector in config["LMs"][LM_id]["partitions"][partition_id]:
					self.WORKER_CONSTRAINTS_VECTOR[LM_id][partition_id].append(BitArray(constraint_vector))
				if partition_id==self.GM_id:
					self.internal_available_nodes[LM_id]=BitArray("0b"+'1'*self.PARTITON_SIZE)
				else:
					self.external_available_nodes[LM_id][partition_id]=BitArray("0b"+'1'*self.PARTITON_SIZE)

				#populate per worker constraints dictionary
				with open("logs/node_constraints.txt", "a") as file:	
					for node_id in range(0,self.PARTITON_SIZE):
						key=LM_id+"_"+partition_id+"_"+str(node_id)
						constraints=self.get_node_constraints(node_id,self.WORKER_CONSTRAINTS_VECTOR[LM_id][partition_id])
						self.WORKER_CONSTRAINTS[key]=constraints
						file.write(key+",")
						file.write(",".join(str(constraint) for constraint in constraints ))
						file.write("\n")

		# print(self.WORKER_CONSTRAINTS)
		debug_print(f"GM {self.GM_id} initialised")

	def get_node_constraints(self,node_id,partition):
		node_constraints=list()
		for constraint,constraint_vector in enumerate(partition):
			if constraint_vector[node_id]:
				node_constraints.append(constraint)
		return node_constraints


	def schedule_job_batched_all(self, job, current_time):
		job.gm = self
		job_id=job.job_id
		self.jobs[job_id]=job
		delete_task_ids=[]
		task_mapping_request_batch=[] # holds all requests per LM
		prev_LM=None
		mode=None
		if len(self.internal_busy_partitions)<self.simulation.NUM_LMS :
			if len(self.external_busy_partitions)<self.simulation.NUM_LMS*(self.simulation.NUM_GMS-1):
				mode="A"
			else:
				mode="I"
		elif len(self.external_busy_partitions)<self.simulation.NUM_LMS*(self.simulation.NUM_GMS-1):
			mode="E"
		# print("Mode:",mode,self.internal_busy_partitions,self.external_busy_partitions)
		for task_id in self.jobs[job_id].tasks:
			flag=False
			task=self.jobs[job_id].tasks[task_id]
			for constraint_set in self.WORKER_CONSTRAINTS_SET:
				if set(task.constraints).issubset(constraint_set):
					flag=True
					break
			if not flag:
				delete_task_ids.append(task_id)
				# print(self.GM_id,"TASK HAS NO RESOURCES SATISFYING CONSTRAINTS",task.job.job_id,task.task_id)
				continue
			if job.ideal_completion_time<task.duration:
				job.ideal_completion_time=task.duration

			#Find appropriate node
			#internal partitions available
			task_mapping_request=None

			if mode=="I":
				task_mapping_request=self.schedule_task(current_time,task)

			elif mode=="A":
				task_mapping_request=self.schedule_task(current_time,task)
				if not task_mapping_request and len(self.external_busy_partitions)<=self.simulation.NUM_LMS*(self.simulation.NUM_GMS-1):
					task_mapping_request=self.batched_repartition(current_time,task)

			elif mode=="E":
				task_mapping_request=self.batched_repartition(current_time,task)

			if task_mapping_request is None:
				# print(self.GM_id,"Queuing task:",task)
				self.task_queue.append(task)
				continue

			# is batch complete?
			if prev_LM == task_mapping_request["LM_id"]:
				task_mapping_request_batch.append(task_mapping_request)
			elif prev_LM is None:
				prev_LM=task_mapping_request["LM_id"]
				task_mapping_request_batch.append(task_mapping_request)
			else:
				self.simulation.event_queue.put(
					(current_time+NETWORK_DELAY, VerifyRequestsEvent(
					task_mapping_request_batch,
					self,
					self.simulation.lms[prev_LM],
					)))
				prev_LM=task_mapping_request["LM_id"]
				task_mapping_request_batch=[]
				task_mapping_request_batch.append(task_mapping_request)


			if len(task_mapping_request_batch)==100:
				self.simulation.event_queue.put(
					(current_time+NETWORK_DELAY, VerifyRequestsEvent(
					task_mapping_request_batch,
					self,
					self.simulation.lms[prev_LM],
					)))
				prev_LM=None
				task_mapping_request_batch=[]

		#if all tasks done processing, send batch
		if task_mapping_request_batch:
			self.simulation.event_queue.put(
					(current_time+NETWORK_DELAY, VerifyRequestsEvent(
					task_mapping_request_batch,
					self,
					self.simulation.lms[prev_LM],
					)))

		#delete tasks whose constraints cannot be satisfied by the DC
		for task_id in delete_task_ids:
			del self.jobs[job_id].tasks[task_id]
			self.jobs[job_id].num_tasks-=1
			if(len(self.jobs[job_id].tasks)==0):
				print(self.GM_id,"JOB DELETED",job_id)
			

	def find_constraint_match(self,availability_vector,partition_constraints,task,LM_id,partition_id):
		#do bitwise and across constraint vectors for constraints in task
		resultant_vector=BitArray("0b"+"1"*self.PARTITON_SIZE)
		for constraint in task.constraints:
			resultant_vector= resultant_vector & partition_constraints[constraint]
		if resultant_vector.int == 0: # no nodes with required constraints in the cluster
			task.no_match.add((LM_id,partition_id))
		resultant_vector=resultant_vector&availability_vector
		suitable_nodes=[i for i, bit in enumerate(resultant_vector) if bit]
		return suitable_nodes


	def schedule_task(self, current_time: float,task):
		suitable_nodes=False
		suitable_LM=False
		for LM_id in self.LMs_list:
			if((LM_id,self.GM_id) in task.no_match):
				continue
			if LM_id in self.internal_busy_partitions:
				continue
			if self.internal_available_nodes[LM_id].int!=0:
				suitable_nodes=self.find_constraint_match(self.internal_available_nodes[LM_id],self.WORKER_CONSTRAINTS_VECTOR[LM_id][self.GM_id],task,LM_id,self.GM_id)
				if suitable_nodes:
					suitable_LM=LM_id
					break
			else:
				self.internal_busy_partitions.add(LM_id)
				continue

		if not suitable_LM:
			return None
		node_id=random.choice(suitable_nodes)
		self.internal_available_nodes[suitable_LM][node_id]=False
		task_id=task.task_id
		task.node_id=node_id
		task.partition_id=self.GM_id
		task.GM_id=self.GM_id
		task.lm=self.simulation.lms[suitable_LM]
		# print(self.GM_id,"Mapping found:",task,suitable_LM,self.GM_id,node_id)
		# print(self.GM_id,self.internal_available_nodes)
		return {"task":task,"LM_id":suitable_LM}

	
	def batched_repartition(self,current_time: float, task):
		suitable_nodes=False
		suitable_partition=False
		for LM_id in self.LMs_list:
			for GM_id in self.GMs_list:
				if GM_id==self.GM_id:
					continue
				if((LM_id,GM_id) in task.no_match):
					continue
				if(LM_id+"_"+GM_id in self.external_busy_partitions):
					continue
				if self.external_available_nodes[LM_id][GM_id].int !=0:
					suitable_nodes=self.find_constraint_match(self.external_available_nodes[LM_id][GM_id],self.WORKER_CONSTRAINTS_VECTOR[LM_id][GM_id],task,LM_id,GM_id)	
					if suitable_nodes:
						suitable_partition=(LM_id,GM_id)
						break
				else:
					self.external_busy_partitions.add(LM_id+"_"+GM_id)
			if suitable_partition:#suitable partition found
				break

		if not suitable_partition:
			return None
		else:
			node_id=random.choice(suitable_nodes)

			self.external_available_nodes[suitable_partition[0]][suitable_partition[1]][node_id]=False
			task_id=task.task_id
			job=task.job
			task.node_id=node_id
			task.partition_id=suitable_partition[1]
			task.GM_id=self.GM_id
			task.lm=self.simulation.lms[suitable_partition[0]]
			# print(self.GM_id,"External mapping found:",task,suitable_partition[0],suitable_partition[1],node_id)
			# print(self.GM_id,self.external_available_nodes)
			return{"task":task,"LM_id":suitable_partition[0]}

	
	def receive_task_response(self,current_time: float, completed_task: Task):
		# print(completed_task,completed_task.job.job_id)
		job_id=completed_task.job.job_id
		job=self.jobs[job_id]
		job.num_tasks-=1
		if completed_task in job.completed_tasks:
			print("Error. Duplication.", task.task_id,job.job_id)
			exit()
		with open("logs/tasks.txt", "a") as file:
				file.write(job.job_id+","+completed_task.task_id+","+str(completed_task.start_time))
				file.write(","+str(current_time)+"\n")
		job.completed_tasks.append(completed_task)
		# print(self.GM_id,current_time,",TC,",completed_task,completed_task.lm.LM_id,completed_task.partition_id,completed_task.node_id)
		del self.jobs[job_id].tasks[completed_task.task_id]
		
		if not self.jobs[job_id].tasks:  # no more tasks left
			assert completed_task.end_time is not None
			assert job.completion_time is not None
			job.completion_time = current_time
			job.end_time = job.completion_time
			with open("logs/JRT.txt", "a") as file:
				file.write(str(current_time)+",")
				file.write(job.job_id+",")
				file.write(str(current_time-job.start_time-job.ideal_completion_time))
				file.write("\n")
			GM.jobs_completed+=1
			print(self.GM_id,self.GM_id,current_time,",JC,",job_id,",",job.completion_time-job.start_time-job.ideal_completion_time)


		#match free resource to a pending task to reduce complexity
		if(self.task_queue):
			node_constraints=self.WORKER_CONSTRAINTS[completed_task.lm.LM_id+"_"+completed_task.partition_id+"_"+str(completed_task.node_id)]
			for task_index in range(0,len(self.task_queue)):
				new_task=self.task_queue[task_index]

				if set(new_task.constraints).issubset(node_constraints):
					new_job=new_task.job
					key = PartitionKey(gm_id=completed_task.partition_id, lm_id=completed_task.lm.LM_id)
					self.task_queue.pop(task_index)
					new_task.partition_id=completed_task.partition_id
					new_task.GM_id=self.GM_id
					new_task.node_id=completed_task.node_id
					new_task.lm=self.simulation.lms[completed_task.lm.LM_id]
					# print(self.GM_id,"TC matched:",new_task,new_task.lm.LM_id,new_task.partition_id,new_task.node_id)
					self.simulation.event_queue.put(
						(current_time+NETWORK_DELAY, VerifyRequestsEvent(
						[{"task":new_task,"LM_id":completed_task.lm.LM_id}],
						self,
						self.simulation.lms[completed_task.lm.LM_id]
						)))
					return

		#free resources - no takers
		#or - no tasks waiting in the queue

		if(completed_task.partition_id==self.GM_id):
			self.internal_available_nodes[completed_task.lm.LM_id][completed_task.node_id]=True
			self.internal_busy_partitions.discard(completed_task.lm.LM_id)
		else:
			self.external_available_nodes[completed_task.lm.LM_id][completed_task.partition_id][completed_task.node_id]=True
			self.external_busy_partitions.discard(completed_task.lm.LM_id+"_"+completed_task.partition_id)
	
	def update_status(self,lm:LM,latest_LM_config,current_time):

		LM_id=lm.LM_id
		flag=False
		for partition_id in latest_LM_config:
			busy_nodes=latest_LM_config[partition_id]["busy"]
			for node_id in busy_nodes:
				if partition_id==self.GM_id:
					self.internal_available_nodes[LM_id][node_id]=False
				else:
					self.external_available_nodes[LM_id][partition_id][node_id]=False

			available_nodes=latest_LM_config[partition_id]["available"]
			for node_id in available_nodes:
				task_index=0
				flag=False
				while task_index <len(self.task_queue):
					task=self.task_queue[task_index]
					node_constraints=self.WORKER_CONSTRAINTS[LM_id+"_"+partition_id+"_"+str(node_id)]
					if set(task.constraints).issubset(node_constraints):
						flag=True
						key = PartitionKey(gm_id=partition_id, lm_id=LM_id)
						self.task_queue.pop(task_index)
						task.node_id=node_id
						task.partition_id=partition_id
						task.GM_id=self.GM_id
						task.lm=self.simulation.lms[LM_id]
						# print(self.GM_id,"US:",task,task.lm.LM_id,task.partition_id,task.node_id)
						self.simulation.event_queue.put(
							(current_time+NETWORK_DELAY, VerifyRequestsEvent(
							[{"task":task,"LM_id":LM_id}],
							self,
							self.simulation.lms[LM_id]
							)))
						break	 
					task_index+=1
				if not flag:# no match found:
					if partition_id==self.GM_id:
						self.internal_available_nodes[LM_id][node_id]=True
						self.internal_busy_partitions.discard(LM_id)
					else:
						self.external_available_nodes[LM_id][partition_id][node_id]=True
						self.external_busy_partitions.discard(LM_id+"_"+partition_id)
						

	def unschedule_task(self, unverified_task: Task):
		"""
		Job is inserted back into the task_queue of the GM.
		"""
		unverified_task.partition_id=None
		unverified_task.node_id=None
		unverified_task.lm=None
		# print(self.GM_id,"Inconsistent:",unverified_task)
		self.task_queue.insert(0,unverified_task)
		