import pickle
import queue
import json
from typing import Dict
import random

from bitstring import BitArray
from master import Master
from distributor import Distributor
from job import Job
# from simulation_logger import SimulationLogger
from simulator_utils.values import TaskDurationDistributions
from events import JobArrivalEvent
from simulator_utils import debug_print

import os
import sys

WORKLOAD_FILE= sys.argv[1]

WORKLOAD_FILE_NAME = "subtrace_"+(os.path.basename(WORKLOAD_FILE)
                                      .split("_")[-1])

# logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()


class Simulation(object):
    def __init__(
            self,
            workload,
            config
           ):

        
        self.config = json.load(open(config))
        self.WORKLOAD_FILE = workload
        self.NUM_masters=int(self.config["num_masters"])
        self.NUM_distributors=int(self.config["num_distributors"])
        self.total_workers = int(self.config["num_workers"])
        self.cutoff=float(self.config["cutoff"])
        self.jobs = {}
        self.event_queue = queue.PriorityQueue()
        self.rand_obj=random.Random()
        self.distributors = {}
        self.workers=dict()
        self.task_occurrence_type={} #weights for generating task placement constraints
        counter = 1
        WORKERS_CONSTRAINT_SETS=dict()
        for master_id in self.config["workers"]:
            WORKERS_CONSTRAINT_SETS[master_id]=dict()
            for worker_id in range(0,int(self.config["num_workers"])//int(self.config["num_masters"])):
                worker_constraints=set()
                for constraint,constraint_vector in enumerate(self.config["workers"][master_id]):
                    if BitArray(constraint_vector)[worker_id]:
                        worker_constraints.add(constraint)
                with open("logs/node_constraints.txt", "a") as file:    
                        key=master_id+"_"+str(worker_id)
                        file.write(key+",")
                        file.write(",".join(str(constraint) for constraint in worker_constraints ))
                        file.write("\n")
                flag=False
                for key in WORKERS_CONSTRAINT_SETS[master_id].keys():
                    if set(worker_constraints).issubset(key):
                    # If the set is a subset of the key, increment the corresponding value
                        WORKERS_CONSTRAINT_SETS[master_id][key] += 1
                        flag=True
                if not flag:
                    WORKERS_CONSTRAINT_SETS[master_id][tuple(worker_constraints)]=1
        while len(self.distributors) < self.NUM_distributors:
            self.distributors[str(counter)] = Distributor(self, str(counter), WORKERS_CONSTRAINT_SETS)  # create deep copy
            counter += 1

        # initialise masters
        self.masters: Dict[str, Master] = {}
        counter = 1

        while len(self.masters) < self.NUM_masters:
            self.masters[str(counter)] = Master(self,
                                        str(counter),
                                        pickle.loads(
                                            pickle.dumps(self.config)))  # create deep copy

            # debug_print(f"Master - {counter} "
            #             f"{self.masters[str(counter)].get_free_cpu_count_per_distributor()}")
            counter += 1

        

        self.jobs_scheduled = 0
        self.jobs_completed = 0
        self.scheduled_last_job = False
        self.tcfv={
                "1":[8,7,7,10,23,20,30,1,3,1,4,3,2,1,4,2,9,8,11,25,29],
                "2":[7.5,6.6,6.6,9.4,21.6,18.8,28.2,0.9,2.8,0.9,3.8,2.8,1.9,0.9,3.8,1.9,8.5,7.5,10.3,23.5,27.3],
                "3":[7.8,6.9,6.9,9.8,22.5,19.6,29.4,1,2.9,1,3.9,2.9,2,1,3.9,2,8.8,7.8,10.8,24.5,28.4],
                "4":[5.8,5,5,7.2,16.6,14.4,21.6,0.7,2.2,0.7,2.9,2.2,1.4,0.7,2.9,1.4,6.5,5.8,7.9,18,20.9],
                "5":[9.7,8.5,8.5,12.1,27.8,24.2,36.3,1.2,3.6,1.2,4.8,3.6,2.4,1.2,4.8,2.4,10.9,9.7,13.3,30.2,35.1],
                "6":[6.6,5.7,5.7,8.2,18.9,16.4,24.6,0.8,2.5,0.8,3.3,2.5,1.6,0.8,3.3,1.6,7.4,6.6,9,20.5,23.8],
                "7":[8.7,7.6,7.6,10.9,25.1,21.8,32.7,1.1,3.3,1.1,4.4,3.3,2.2,1.1,4.4,2.2,9.8,8.7,12,27.3,31.6],
                "8":[4.7,4.1,4.1,5.9,13.6,11.8,17.7,0.6,1.8,0.6,2.4,1.8,1.2,0.6,2.4,1.2,5.3,4.7,6.5,14.8,17.1],
                "9":[5.7,5,5,7.1,16.3,14.2,21.3,0.7,2.1,0.7,2.8,2.1,1.4,0.7,2.8,1.4,6.4,5.7,7.8,17.8,20.6],
                "10":[7.2,6.3,6.3,9,20.6,17.9,26.9,0.9,2.7,0.9,3.6,2.7,1.8,0.9,3.6,1.8,8.1,7.2,9.9,22.4,26]
            }
        task_occurrence={'1': 
                    {'1': 0.039628712871287115, 
                    '2': 0.12043316831683168, 
                    '3': 0.13037128712871288, 
                    '4': 0.11675742574257425}, 
                '2': {'1': 0.1404950495049505, 
                    '2': 0.07971534653465345, 
                    '3': 0.16081683168316832, 
                    '4': 0.12047029702970297}, 
                '3': {'1': 0.05905940594059404, 
                    '2': 0.11016089108910891, 
                    '3': 0.1506683168316832, 
                    '4': 0.13383663366336634},
                 '4': {'1': 0.12032178217821782, 
                    '2': 0.12043316831683168, 
                    '3': 0.049306930693069295, 
                    '4': 0.09658415841584157}, 
                 '5': {'1': 0.08987623762376236, 
                    '2': 0.09976485148514852, 
                    '3': 0.16081683168316832, 
                    '4': 0.11675742574257425}, 
                 '6': {'1': 0.11004950495049505, 
                    '2': 0.09976485148514852, 
                    '3': 0.12047029702970297, 
                    '4': 0.1101980198019802}, 
                 '7': {'1': 0.08987623762376236, 
                    '2': 0.059294554455445535, 
                    '3': 0.028886138613861374, 
                    '4': 0.049306930693069295}, 
                 '8': {'1': 0.1404950495049505, 
                    '2': 0.13033415841584156, 
                    '3': 0.049306930693069295,
                     '4': 0.10388613861386138}, 
                 '9': {'1': 0.12032178217821782,
                     '2': 0.09011138613861384, 
                     '3': 0.12047029702970297,
                      '4': 0.09980198019801981},
                 '10': {'1': 0.08987623762376236, 
                     '2': 0.08998762376237622, 
                     '3': 0.028886138613861374, 
                     '4': 0.052400990099009885}
            }

        #convert to task occurrence weights per type
        for i in ["1","2","3","4"]:
            self.task_occurrence_type[i]=list()
            for j in ["1","2","3","4","5","6","7","8","9","10"]:
                self.task_occurrence_type[i].append(task_occurrence[j][i])

        print("Simulation instantiated")

                
 # Simulation class
    def run(self):
        last_time = 0
    
        self.jobs_file = open(self.WORKLOAD_FILE, 'r')

        self.task_distribution = TaskDurationDistributions.FROM_FILE

        line = self.jobs_file.readline()  # first job
        new_job = Job(self.task_distribution, line, self)
        self.event_queue.put((float(line.split()[0]), JobArrivalEvent(
            self, self.task_distribution, new_job, self.jobs_file)))
        
        self.jobs_scheduled = 1

        # start processing events
        while (not self.event_queue.empty()):
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            event.run(current_time)
        print("Simulation ending, no more events")
        for master_id in self.masters:
            print(len(self.masters[master_id].high_priority_task_queue),len(self.masters[master_id].low_priority_task_queue))
        # logger.info("Simulator Info , Simulation ending, no more events")
        self.jobs_file.close()
