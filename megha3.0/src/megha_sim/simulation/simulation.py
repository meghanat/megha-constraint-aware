import pickle
import queue
import json
from typing import Dict
from bitstring import BitArray

from local_master import LM
from global_master import GM
from job import Job
# from simulation_logger import SimulationLogger
from simulator_utils.values import TaskDurationDistributions
from events import JobArrivalEvent, LMRequestUpdateEvent, InconsistencyEvent
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

        # Each localmaster has one partition per global master so the total number of partitions in the cluster are:
        # NUM_GMS * NUM_LMS
        # Given the number of worker nodes per partition is PARTITION_SIZE
        # the total_nodes are NUM_GMS*NUM_LMS*PARTITION_SIZE
        self.config = json.load(open(config))
        self.WORKLOAD_FILE = workload
        self.NUM_LMS=len(self.config["LMs"])
        self.NUM_GMS=len(self.config["LMs"]["1"]["partitions"])
        self.PARTITION_SIZE=len(self.config["LMs"]["1"]["partitions"]["1"][0])-2 #(-2 for the "0b" string in the constraint vector)
        self.total_nodes = self.NUM_GMS * self.NUM_LMS * self.PARTITION_SIZE
        self.task_occurrence_type={} #weights for generating task placement constraints
        self.jobs = {}
        self.event_queue = queue.PriorityQueue()
        self.job_queue=[]
        # initialise GMs
        self.gms = {}
        counter_gm = 1
        WORKERS_CONSTRAINT_SETS=list()
        
        for LM_id in range(1,self.NUM_LMS+1):
             for GM_id in range(1,self.NUM_GMS+1):   
                for worker_id in range(0,self.PARTITION_SIZE):
                    worker_constraints=set()
                    for constraint,constraint_vector in enumerate(self.config["LMs"][str(LM_id)]["partitions"][str(GM_id)]):
                        if BitArray(constraint_vector)[worker_id]:
                            worker_constraints.add(constraint)
                    # print(worker_constraints)
                    flag=False
                    for item in WORKERS_CONSTRAINT_SETS:
                        if set(worker_constraints).issubset(item):
                            flag=True
                            break
                    if not flag:
                        WORKERS_CONSTRAINT_SETS.append(worker_constraints)
        while len(self.gms) < self.NUM_GMS:
            self.gms[str(counter_gm)] = GM(self, str(counter_gm),pickle.loads(
                pickle.dumps(self.config)),WORKERS_CONSTRAINT_SETS)  # create deep copy
            counter_gm += 1

        # initialise LMs
        self.lms: Dict[str, LM] = {}
        counter = 1

        while len(self.lms) < self.NUM_LMS:
            self.lms[str(counter)] = LM(self,
                                        str(counter),
                                        self.PARTITION_SIZE,
                                        pickle.loads(
                                            pickle.dumps(self.config["LMs"][str(counter)])))  # create deep copy
            counter += 1

        self.shared_cluster_status = {}

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
            
 # Simulation class
    def run(self):
        last_time = 0
    
        self.jobs_file = open(self.WORKLOAD_FILE, 'r')

        self.task_distribution = TaskDurationDistributions.FROM_FILE

        line = self.jobs_file.readline()  # first job
        new_job = Job(self.task_distribution, line, self)
        # starting the periodic LM updates
        self.event_queue.put((float(line.split()[0])-0.05, LMRequestUpdateEvent(self)))
        self.event_queue.put((float(line.split()[0]), JobArrivalEvent(
            self, self.task_distribution, new_job, self.jobs_file)))
        
        
        self.jobs_scheduled = 1

        # start processing events
        while (not self.event_queue.empty()):
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            if(new_events is None):
                continue
            for new_event in new_events:
                if(new_event is None):
                    continue
                self.event_queue.put(new_event)

        # print("Simulation ending, no more events")
        # logger.info("Simulator Info , Simulation ending, no more events")
        self.jobs_file.close()
        print("Jobs completed:",GM.jobs_completed)
