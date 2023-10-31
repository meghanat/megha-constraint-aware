import json
from typing import List, Tuple
from simulator_utils.values import NETWORK_DELAY
from simulator_utils import debug_print
from events import  TaskArrivedAtWorkerEvent
import pickle
from bitstring import BitArray
import random
class Master(object):

	def __init__(self, simulation, master_id, config):
		self.master_id = master_id
		self.simulation = simulation
		self.total_workers=int(config["num_workers"])
		self.num_master_workers=self.total_workers//int(config["num_masters"])
		self.reserve=float(config["reserve"])
		self.FQW=float(config["FQW"])
		self.fq_counter=0
		self.low_priority_task_queue=list()
		self.high_priority_task_queue=list()
		self.availability_vector=BitArray("0b"+"1"*self.num_master_workers)
		self.config=list()
		for constraint_vector in config["workers"][master_id]:
			self.config.append(BitArray(constraint_vector)) 
		self.WORKER_CONSTRAINTS=dict()
		for worker in range(0,self.num_master_workers):
			self.WORKER_CONSTRAINTS[worker]=[]
			for constraint_id in range(0,len(config["workers"][master_id])):
				constraint_vector=self.config[constraint_id]
				if constraint_vector[worker]:
					self.WORKER_CONSTRAINTS[worker].append(constraint_id)
		debug_print(f"Master {master_id} initialised")
		
		
	def schedule_task(self,task,current_time):
		suitable_workers=[]
		if self.availability_vector.int!=0:
			resultant_vector=BitArray("0b"+"1"*self.num_master_workers)
			for constraint in task.constraints:
				resultant_vector=resultant_vector & self.config[constraint]
			if resultant_vector.int ==0:
				print("Error, no suitable_workers")
				exit()
			resultant_vector=resultant_vector&self.availability_vector
			suitable_workers=[i for i, bit in enumerate(resultant_vector) if bit]
		if not suitable_workers:#no available workers
			# print(self.master_id,current_time,"queueing task:",task)
			if task.is_high_priority:
				self.high_priority_task_queue.append(task)
			else:
				self.low_priority_task_queue.append(task)
		else:
			min_constraints=10000
			min_worker=None
			for worker in suitable_workers:
				constraint_len=len(self.WORKER_CONSTRAINTS[worker])
				if min_constraints>constraint_len:
					min_constraints=constraint_len
					min_worker=worker
			worker=min_worker

			if(not self.availability_vector[worker]):
				print("Error")
				exit()
			self.availability_vector[worker]=False
			# print(self.master_id,current_time,"Schedule Task:",task,worker,self.WORKER_CONSTRAINTS[worker])
			task.worker=worker
			self.simulation.event_queue.put((current_time+NETWORK_DELAY,TaskArrivedAtWorkerEvent(
											 self.simulation,task)))


	def idle_worker_notice(self,completed_task,current_time):
		worker=completed_task.worker
		# print(current_time,"TC:",completed_task,"worker:",worker,self.WORKER_CONSTRAINTS[worker])
		if (self.fq_counter<=self.FQW) or len(self.low_priority_task_queue)==0:
			#search in HPQ for task having matching constraints
			for pending_task_index in range(0,len(self.high_priority_task_queue)):
				pending_task=self.high_priority_task_queue[pending_task_index]
				# print("pending_task:",pending_task.job.job_id,pending_task.task_id,pending_task.constraints)
				if set(pending_task.constraints).issubset(self.WORKER_CONSTRAINTS[completed_task.worker]):
					self.fq_counter=self.fq_counter+1
					pending_task.worker=completed_task.worker
					self.high_priority_task_queue.pop(pending_task_index)
					# print(self.master_id,"HPQ",current_time,"Idle Worker Notice:",pending_task,"worker constraints:",self.WORKER_CONSTRAINTS[pending_task.worker])
					self.simulation.event_queue.put((current_time+NETWORK_DELAY,TaskArrivedAtWorkerEvent(self.simulation,pending_task)))
					return
		for pending_task_index in range(0,len(self.low_priority_task_queue)):
			pending_task=self.low_priority_task_queue[pending_task_index]
			if set(pending_task.constraints).issubset(self.WORKER_CONSTRAINTS[completed_task.worker]):
				self.fq_counter=0
				pending_task.worker=completed_task.worker
				self.low_priority_task_queue.pop(pending_task_index)
				# print(self.master_id,"LPQ",current_time,"Idle Worker Notice:",pending_task,"worker constraints:",self.WORKER_CONSTRAINTS[pending_task.worker])
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,TaskArrivedAtWorkerEvent(self.simulation,pending_task)))
				return
		for pending_task_index in range(0,len(self.high_priority_task_queue)):
			pending_task=self.high_priority_task_queue[pending_task_index]
			# print("pending_task:",pending_task.job.job_id,pending_task.task_id,pending_task.constraints)
			if set(pending_task.constraints).issubset(self.WORKER_CONSTRAINTS[completed_task.worker]):
				self.fq_counter=self.fq_counter+1
				pending_task.worker=completed_task.worker
				self.high_priority_task_queue.pop(pending_task_index)
				# print(self.master_id,"HPQ",current_time,"Idle Worker Notice:",pending_task,"worker constraints:",self.WORKER_CONSTRAINTS[pending_task.worker])
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,TaskArrivedAtWorkerEvent(self.simulation,pending_task)))
				return
		
		self.availability_vector[completed_task.worker]=True
