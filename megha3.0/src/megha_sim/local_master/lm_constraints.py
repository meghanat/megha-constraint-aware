import json
from typing import List, Tuple
from bitstring import BitArray
from simulator_utils.values import NETWORK_DELAY, InconsistencyType
import simulator_utils.globals
from simulator_utils import debug_print
from events import LaunchOnNodeEvent,LMStatusUpdateEvent, BatchedInconsistencyEvent,InconsistencyEvent, LMRequestUpdateEvent, TaskResponseEvent
from megha_sim.global_master.gm_types import LMResources
import pickle

class LM(object):

	def __init__(self, simulation, LM_id, partiton_size, LM_config):
		self.LM_id = LM_id
		self.partiton_size = partiton_size
		self.LM_config = {}
		self.LM_config["LM_id"]=LM_config["LM_id"]
		self.LM_config["partitions"]={}
		self.status_update={}
		for partition_id in LM_config["partitions"]:
			self.LM_config["partitions"][partition_id]=BitArray("0b"+"1"*self.partiton_size)

		debug_print(f"LM {LM_id} initialised")
		self.simulation = simulation
		for GM_id in self.simulation.gms:
			self.status_update[GM_id]={}
			for partition_id in LM_config["partitions"]:
				self.status_update[GM_id][partition_id]={"available":[],"busy":[]}
	
	def send_status_update(self,current_time):
		self.simulation.event_queue.put((current_time + NETWORK_DELAY, LMStatusUpdateEvent(json.loads(json.dumps(self.status_update)), self.simulation,self)))
		for GM_id in self.simulation.gms:
			self.status_update[GM_id]={}
			for partition_id in self.LM_config["partitions"]:
				self.status_update[GM_id][partition_id]={"available":[],"busy":[]}

	# # LM checks if GM's single request is valid
	# def verify_request(self,task,gm,current_time):
	# 	node_id=int(task.node_id)
	# 	task.lm=self
	# 	if (self.LM_config["partitions"][task.partition_id][node_id]):
	# 		self.LM_config["partitions"][task.partition_id][node_id] = False
	# 		self.simulation.event_queue.put(
	# 			(current_time + NETWORK_DELAY, LaunchOnNodeEvent(task, self.simulation)))
	# 		return True
	# 	else:  # if inconsistent
	# 			print(self.LM_id,":InconsistencyEvent. node:",task.node_id," job/task:",task.job.job_id,"/",task.task_id)
	# 			exit()
	# 			self.simulation.event_queue.put((current_time+ NETWORK_DELAY, InconsistencyEvent(
	# 				task, gm,self, self.status_update[gm.GM_id],self.simulation)))
	# 			for partition_id in self.LM_config["partitions"]:
	# 				self.status_update[gm.GM_id][partition_id]=[]

	# LM checks if GM's batched request is valid	
	def verify_requests(self,task_mappings,gm,current_time):
		inconsistent_mappings=[]

		for task_mapping in task_mappings:
			task=task_mapping["task"]
			task.lm=self
			node_id=int(task.node_id)
			
			if (self.LM_config["partitions"][task.partition_id][task.node_id]):
				self.LM_config["partitions"][task.partition_id][task.node_id]= False
				self.simulation.event_queue.put(
                    (current_time + NETWORK_DELAY, LaunchOnNodeEvent(task, self.simulation)))
				for GM_id in self.simulation.gms:
					if GM_id==task.GM_id:
						continue
					else:
						self.status_update[GM_id][task.partition_id]["busy"].append(task.node_id)
			else:
				
				# print(self.LM_id,"1BatchedInconsistencyEvent:",task.job.job_id,task.task_id,self.LM_id+"_"+task.partition_id+"_"+str(task.node_id))
				inconsistent_mappings.append(task_mapping)
				with open("logs/inconsistencies.txt", "a") as file:
					file.write(str(current_time)+"1\n")
		if inconsistent_mappings:
			self.simulation.event_queue.put((current_time+ NETWORK_DELAY, BatchedInconsistencyEvent(
					inconsistent_mappings, gm, self, self.simulation,self.status_update[gm.GM_id])))
			for partition_id in self.LM_config["partitions"]:
				self.status_update[gm.GM_id][partition_id]={"available":[],"busy":[]}

	def task_completed(self, task):
		
		# reclaim resources
		self.LM_config["partitions"][task.partition_id][task.node_id]= True
		for GM_id in self.simulation.gms:
			if GM_id==task.GM_id:
				continue
			else:
				self.status_update[GM_id][task.partition_id]["available"].append(task.node_id)
		
		self.simulation.event_queue.put((task.end_time + NETWORK_DELAY,
										 TaskResponseEvent(
											 self.simulation,
											 self.simulation.gms[task.GM_id],task)))

   