# YACS (Yet Another Centralized Schedule)

**Big Data Project** - Semester 5, December 2020

---
## Procedure to execute

* **master.py**: 
  <br>python3 master.py <algorithm>
  * algorithm:
    * 1 --> Random
    * 2 --> Round-Robin
    * 3 --> Least Loaded
* **worker.py**: <br>python3 worker.py<br>
* **analysis.py**: <br>python3 analysis.py

---
## Introduction
### Aim
* Simulate the working of a centralized scheduling framework.
* Implement the functioning of 3 different scheduling algorithms.
* Calculate metrics and analyze the results.
### Overview
* Big data workloads consist of multiple jobs from different applications. 
* These workloads are too large to run on a single machine.
* They run on clusters of interconnected machines. 
* A scheduling framework is used to manage and allocate the resources of the cluster (CPUs, memory, disk, network bandwidth, etc.) to the different jobs in the workload.
* A job is made of one or more tasks. 
* The scheduling framework receives job requests and launches the tasks in the jobs on machines in the cluster. 
* In the figure, Task 3 of the orange job is assigned to the free resources on Machine 3.
* Once the task is allocated resources on a machine, it executes its code. 
* When a task finishes execution, the scheduling framework is informed, and the resources are freed. 
* The framework can thereafter assign these freed resources to other tasks. 
### YACS
* The  final project is focused on YACS, a **centralized scheduling framework**.
* The framework consists of one **Master**, which runs on a dedicated machine and manages the resources of the rest of the machines in the cluster. 
* The other machines in the cluster have one **Worker process** running on each of them . 
* The Master process makes scheduling decisions while the Worker processes execute the tasks and inform the Master when a task completes its execution.
* The Master listens for job requests and dispatches the tasks in the jobs to machines based on a scheduling algorithm. 
* Each machine is partitioned into equal-sized resource encapsulations (E.g. 1 CPU core, 4GB of memory, 1TB of disk space) called slots. 
* The number of slots in each machine is fixed. Each slot has enough resources to execute one task at a time. 
* Machines may have different capacities (in terms of amount of total memory, total number of cores, etc.); the number of slots may differ from machine to machine. 
* Maximum number of tasks a specific machine can run is equal to the number of slots in the machine. 
* The Master is informed of the number of machines and the number of slots in each machine with the help of a config file.
* The Worker processes listen for Task Launch messages from the Master. 
* On receiving a Launch message, the Worker adds the task to the execution pool of the machine it runs on. 
* The execution pool consists of all currently running tasks in the machine.
* When a task completes execution, the Worker process on the machine informs the Master.
* The Master then updates its information about the number of free slots available on the machine. 
------
## Simulation
### Overview
* Simulate YACS with **1 Master process and 3 Worker processes**.
* All processes will run on the same PC.
* However, must behave as though each is a component deployed on a dedicated machine.
* ++Simulated machine has nothing to do with the PC on which you will be running the simulation.++
* The number of slots in each simulated Worker machine is as per the config passed to the Master process.
### Machine resources (slots)
* In your implementation, slots are only an abstraction. 
* For instance, the value of available slots for a simulated machine is only a number. 
* It is decremented when a slot is said to have been allocated to a task
* It is incremented when a slot is said to have been freed on a task’s completion
### Worker process and task execution
* The Worker process listens for task launch message from the Master.
* When it receives a task launch message, it adds the task to its execution pool. 
* The Worker process must simulate the running of the tasks in the execution pool. 
* It does so by decrementing the remaining_duration value of each task, every second, until it reaches 0. 
* Once the remaining_duration of a task reaches 0, the Worker removes the task from its execution pool and reports to the Master that the task has completed its execution. 
### Inter-task dependency
* The framework will have to respect the map-reduce dependency in the jobs. 
* When a map task completes execution, the Master will have to check if it satisfies the dependencies of any reduce tasks and whether the reduce tasks can now be launched. 
* A job is said to have completed execution only when all the tasks in the job have finished executing.  
* All jobs have 2 stages only - The first stage consists of map tasks and the second stage consists of reduce tasks. 
* The reduce tasks in a job can only execute after all the map tasks in the job have finished executing. 
* There  is no ordering within reduce tasks, or within map tasks. All map tasks can run in parallel, and all reduce tasks can run in parallel. 
----
## Scheduling algorithm
**Random**: The Master chooses a machine at random. It then checks if the machine has free slots available. If yes, it launches the task on the machine. Else, it chooses another machine at random. This process continues until a free slot is found.<br>
**Round-Robin**: The machines are ordered based on worker_id of the Worker running on the machine. The Master picks a machine in round-robin fashion. If the machine does not have a free slot, the Master moves on to the next worker_id in the ordering. This process continues until a free slot is found.<br>
**Least-Loaded**: The Master looks at the state of all the machines and checks which machine has most number of free slots. It then launches the task on that machine. If none of the machines have free slots available, the Master waits for 1 second and repeats the process. This process continues until a free slot is found.<br>