# megha-constraint-aware
The repository contains code and traces used to compare Megha with a constraint-aware version of Pigeon

To run the code, use the following commands in either the megha3.0 folder or the pigeon_sim folder:

`python3 src/runner.py <path to input trace> <path to config>` 

To change the constraint matching approach, include the appropriate file in `src/megha_sim/global_master/__init__.py` or `src/pigeon_sim/master/__init__.py`. For instance, to try the minimum constraints approach with PigeonC, ensure the `master_constraints_minC.py` file is included in `src/pigeon_sim/master/__init__.py`. Remove any mention of the `master_constraints_rand.py` file from the `__init__.py` file.
