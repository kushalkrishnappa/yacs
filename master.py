import json
import socket
import sys
import random
import threading
from datetime import datetime

"""
LOG PATTERN
<sl_no>,<algo>,<code>,<id>,<time>,<worker_id>\n

<sl_no>
    Current execution number

<algo>
    1 - Random
    2 - Round Robin
    3 - Least loaded

<code>
    101 - job arrival
    102 - task start
    201 - job finish
    202 - task finish

<id>
    job_id if its a job
    tasks_id if its a task, irrespective of its type

<worker_id>
    -1 for job
    x  for tasks
"""

class Needs:  # This is class used to share data among master, worker, log file
    worker_object = {}
    # A single lock object is used to lock both log_file and other tasks threads
    lock = threading.Lock()
    # An index used for round robin algorithm
    rr_index = 0
    # this is flag which defines the algorithm used by the master
    algo_type = sys.argv[1]

    def __init__(self) -> None:
        """Whenever we are saving logs of a particular job, we gotta know the 
        serial number of the execution to avoid conflicts between same task id or job id """
        try:
            with open("logs.csv", "r") as f:
                x = sorted(list(f.readlines()))
                if x == []:  # if the log_file is empty, start serial number from 0
                    self.sl_no = 0
                    print("in if")
                else:  # if the log_file isnt empty, start serial number from the next avaliable sl_no
                    x = x[-1]
                    self.sl_no = int(x.split(",")[0]) + 1
        except:
            # if the file doesnt exist, file open method using read mode will return error
            # so start sl_no from 0
            self.sl_no = 0


class Worker:
    """A worker class dedicated to each worker"""

    def __init__(self, dikt):
        self.worker_id = dikt["worker_id"]
        self.slots = dikt["slots"]  # total port number for a worker
        self.port = dikt["port"]
        # availble port at any particular instant of time
        self.available_slot = self.slots
        # this is list of slot which are filled with
        self.filled_slots = []


class Master:
    """A master class dedicated to a one and only master"""

    # This is the queue where all the incoming jobs are dumped
    input_queue = []
    # a queue which contains all the finished tasks, not the jobs
    finished_queue = []

    def __init__(self, needs):
        """When master class is instantiated, thre mian daemons are invoked"""
        self.read_workers(needs)
        # This thread invokes a server whihc always recieves jobs from the clients
        a = threading.Thread(target=self.always_on_server, args=(needs,))
        # this thread invokes a daemon which alwasy tries to empty the input queue
        b = threading.Thread(target=self.duty_is_to_empty_input_queue, args=(needs,))
        # this thread incokes the daemon server which always listens to a particular port numnber which accepts the updates from the
        c = threading.Thread(target=self.read_updates_from_worker, args=(needs,))
        for i in [a, b, c]:
            # all the thread object are started here
            i.start()
        print(
            f"Master has been switched on...\n\tWaiting for jobs to deploy with algo#{needs.algo_type}"
        )
        for i in [a, b, c]:
            # All the thresad objects started above is waiting here to join all the sibling threads
            i.join()
        # this above only terminates when all the threads terminate, which is never gonna happen unless you kill the master process

    def read_workers(self, needs):
        """Initially, we gotta read config.json file, so that master will get
        to know the different workers and the associated portnumber and id of the same"""
        with open("config.json", "r") as f:
            config_json = json.load(f)
        for i in config_json["workers"]:
            w = Worker(i)
            needs.worker_object[i["worker_id"]] = w

    def always_on_server(self, needs):
        """This is a server using TCP connection socket which always tries to listen jobs from the client """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # This socket is bound to the localhost portnumber 5000 as mentioned iun the project specification
        s.bind(("localhost", 5000))
        # Since this is a sevrer, it shouldnt stop working, so we need to use infinite while loop
        while 1:
            try:
                s.listen(1)
                conn, addr = s.accept()
                msg = ""
                # wait until the message receiving has been finished
                while 1:
                    data = conn.recv(2048)
                    if data:
                        msg += data.decode()
                    else:
                        conn.close()
                        break
                    # incoming message has json format, so parse the messsage using json.loads
                msg_json = json.loads(msg)
                # We ought to log the event, and we also need to take care when some other thread is using the same log file
                while 1:
                    if not needs.lock.locked():
                        # if no other thread has locked the log_file, we gotta acquire the lock and then update the log file
                        needs.lock.acquire(True)
                        # Here, we gotta check that the recieved message has at least one job task
                        if msg_json["map_tasks"]:
                            with open("logs.csv", "a") as f:
                                f.write(
                                    f"{needs.sl_no},{needs.algo_type},101,{msg_json['job_id']},{datetime.now()},-1\n"
                                )
                                # insert the job into input_queue
                            self.input_queue.insert(0, msg_json)
                        needs.lock.release()
                        break
            except:
                print("Caught an error in: \n\talways_on_server()")
                pass

    def duty_is_to_empty_input_queue(self, needs):
        """One daemon always dump the jobs into a queue
        This is a daemon which always tries to empty it in FCFS fashion and invokes the job caller"""
        # Each job is threaded in this programmming model
        while 1:
            # Only fif the input queue has a job
            if self.input_queue:
                # we gotta call the job caller only if the slot is empty is in any of the worker
                if self.is_slots_empty(needs):
                    # Pop the first job in the input_queue
                    a_job = self.input_queue.pop(0)
                    # Thread the job called for each job
                    xyz = threading.Thread(target=self.job_caller, args=(a_job, needs,))
                    xyz.start()

    def is_slots_empty(self, needs):
        """Returns true if any slot is empty else false"""
        for i in needs.worker_object.values():
            if i.available_slot > 0:
                return True
        return False

    def job_caller(self, job, needs):
        """This is a function which is responsible for calling a particualr job"""
        map_tasks = job["map_tasks"]  # map lists
        reduce_tasks = job["reduce_tasks"]  # reduce list
        threading_list = []  # threading list
        for i in map_tasks:
            thr = threading.Thread(target=Scheduler.algo_read, args=(i, needs,))
            thr.start()
            threading_list.append(thr)
        for i in threading_list:
            i.join()
        """
        Here we make sure that all the map tasks are finished
        Only when all the map tasks are terminated, reduce tasks start for a particular job
        This is how we tackle the programmin challenges given in the specification sheet
        """
        threading_list = []  # this is the threading list
        for i in reduce_tasks:
            thr = threading.Thread(target=Scheduler.algo_read, args=(i, needs,))
            thr.start()
            threading_list.append(thr)
        for i in threading_list:
            i.join()
        # When the last reduce task is terminated, we conclude that the particular job has been finished.
        # We gotta log this event for further analysis
        while 1:
            # We need to wait for the event to be logged in the log_file
            # if the file lock has already been acquired, we gotta wait
            if not needs.lock.locked():
                needs.lock.acquire(True)
                print(f"{job['job_id']} job finished")
                with open("logs.csv", "a") as f:
                    f.write(
                        f"{needs.sl_no},{needs.algo_type},201,{job['job_id']},{datetime.now()},-1\n"
                    )
                needs.lock.release()
                break

    @classmethod
    def assign_task(self, worker_object, burst_time, task_id, needs):
        """This is the method where tasks are been assigned using the desire scheduling algo"""
        # we gotta log the event, here task has been assigned to a particular worker
        while 1:
            if not needs.lock.locked():
                needs.lock.acquire(True)
                with open("logs.csv", "a") as f:
                    f.write(
                        f"{needs.sl_no},{needs.algo_type},102,{task_id},{datetime.now()},{worker_object.worker_id}\n"
                    )
                needs.lock.release()
                print(f"Sending task {task_id} to {worker_object.worker_id}")
                break
        # if the task has already been finished earlier, we just move on
        if task_id in self.finished_queue:
            return
        # we connect to the particular worker using the hostname and port number
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", worker_object.port))
            # we send the task to execute
            message = f"{worker_object.worker_id},{burst_time},{task_id}"
            s.send(message.encode())
        # we wait until the task has been finished
        while 1:
            if task_id in self.finished_queue:
                if not needs.lock.locked():
                    needs.lock.acquire(True)
                    worker_object.available_slot += 1
                    # once the task has been finished, we gotta log the event
                    with open("logs.csv", "a") as f:
                        f.write(
                            f"{needs.sl_no},{needs.algo_type},202,{task_id},{datetime.now()},{worker_object.worker_id}\n"
                        )
                    needs.lock.release()
                    break

    def read_updates_from_worker(self, needs):
        """This is a daemon server which always tries to listen updates from the worker"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # we bind the server to 5001
        s.bind(("localhost", 5001))
        while 1:
            try:
                s.listen(1)
                conn, addr = s.accept()
                msg = ""
                while 1:
                    data = conn.recv(2048)
                    if data:
                        msg += data.decode()
                    else:
                        conn.close()
                        break
                worker_id, task_id = msg.replace("\n", "").split(",")
                # Add the finished task to the finished queue
                self.finished_queue.append(task_id)
            except:
                print("Caught an error in:\n\tread_updates_from_worker")
                pass


class Scheduler:
    """This is a class which houses all the required schedling algorithm
    Here all the methods are class mthods and we dont need to instantiate the class"""

    def __init__(self):
        # Does nothing, never been executed.
        # if you ever thought that you are waste for the society, image this method
        pass

    @classmethod
    def algo_read(self, task, needs):
        # this examines which scheduling algo must be used
        dikt = {
            "1": self.algo_random,
            "2": self.algo_round_robin,
            "3": self.algo_least_loaded,
        }
        # calls desired algo method
        dikt[needs.algo_type.strip()](task, needs)

    @classmethod
    def algo_random(self, task, needs: Needs):
        """Choose a worker randomly"""
        while True:
            worker_id = random.choice(list(needs.worker_object.keys()))
            worker_obj = needs.worker_object[worker_id]
            if worker_obj.available_slot > 0:  # only if the worker has free slot
                if not needs.lock.locked():  # only if no one has locked
                    needs.lock.acquire(True)  # acquire the lock
                    needs.worker_object[
                        worker_id
                    ].available_slot -= 1  # update shared memory
                    needs.lock.release()
                    Master.assign_task(
                        worker_obj, task["duration"], task["task_id"], needs
                    )
                    break

    @classmethod
    def algo_round_robin(self, task, needs: Needs):
        """Choose a worker in a round robin fashion"""
        while True:
            worker_object_list = list(needs.worker_object.values())
            worker_obj = worker_object_list[needs.rr_index]
            # shared round robin index is used to resume frmo where previous task left
            if worker_obj.available_slot > 0:
                if not needs.lock.locked():
                    needs.lock.acquire(True)
                    worker_obj.available_slot -= 1
                    needs.rr_index = (1 + needs.rr_index) % len(worker_object_list)
                    needs.lock.release()
                    Master.assign_task(
                        worker_obj, task["duration"], task["task_id"], needs
                    )
                    break
            else:
                needs.lock.acquire(True)
                # round robin way
                needs.rr_index = (1 + needs.rr_index) % len(worker_object_list)
                needs.lock.release()

    @classmethod
    def algo_least_loaded(self, task, needs):
        """Choose a worker based on least loaded worker"""
        while True:
            worker_obj = sorted(
                needs.worker_object.values(),
                key=lambda line: (line.slots - line.available_slot),
            )
            # slots - available slot = least_loaded
            worker_obj = worker_obj[0]
            if worker_obj.available_slot > 0:
                if not needs.lock.locked():
                    needs.lock.acquire(True)
                    worker_obj.available_slot -= 1
                    needs.lock.release()
                    Master.assign_task(
                        worker_obj, task["duration"], task["task_id"], needs
                    )
                    break


needs = Needs()
master = Master(needs)
