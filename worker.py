import json
import socket
import time
import threading


class Worker:
    """Worker class for all workers"""

    def __init__(self, id_, port):
        self.id = id_
        self.port = port
        self.task_execution_pool = []
        threading.Thread(target=self.listen_to_tasks).start()
        threading.Thread(target=self.duty_is_to_empty_task_execution_pool).start()

    def listen_to_tasks(self):
        """This is a server using TCP connection socket which always tries to listen tasks from the master """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("localhost", self.port))
        while 1:
            try:
                s.listen(1)
                print(f"\t<:{self.id}:> is waiting for the task-lauch message")
                conn, addr = s.accept()
                msg = ""
                while 1:
                    data = conn.recv(2048)
                    if data:
                        msg += data.decode()
                    else:
                        conn.close()
                        break
                worker_id, burst_time, task_id = msg.replace("\n", "").split(",")
                self.task_execution_pool.append((worker_id, burst_time, task_id))
                print(
                    f"Got a task with id <:{task_id}:> with {burst_time} secs assigned to <:{worker_id}:>"
                )
            except:
                print(f"I, <:{self.id}:> got an error\n\tlisten_to_tasks()")

    def duty_is_to_empty_task_execution_pool(self):
        """One daemon always dump the tasks into a queue
        This is a daemon which always tries to empty it in FCFS fashion and invokes the task executer"""
        while 1:
            if self.task_execution_pool:
                a_task = self.task_execution_pool.pop(0)
                worker_id, burst_time, task_id = a_task
                xyz = threading.Thread(
                    target=self.do_the_task, args=(worker_id, task_id, burst_time,)
                )
                xyz.start()

    def do_the_task(self, worker_id, task_id, burst_time):
        """simluation of task execution"""
        print(f"Now executing <:{task_id}:> for {burst_time} secs on <:{worker_id}:>")
        time.sleep(int(burst_time.strip()))
        while 1:
            try:
                self.send_ack(worker_id, task_id)
                break
            except:
                pass

    def send_ack(self, worker_id, task_id):
        """Gotta send acknowldgement to the master when a particualr task has been finished by the particular worker"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", 5001))
            message = f"{worker_id},{task_id}\n"
            s.send(message.encode())
        print(f"Task <:{task_id}:> has been finished.")


class Solution:
    """An illusion of multiple machines working across the continent"""

    worker_object = []

    def __init__(self):
        with open("config.json", "r") as f:
            config_json = json.load(f)
        for i in config_json["workers"]:
            self.worker_object.append((i["worker_id"], i["port"],))
        # Here we create 3 worker, or n worker given in config file
        self.create_multiple_workers()

    def create_worker(self, worker_id, port):
        w = Worker(worker_id, port)

    def create_multiple_workers(self):
        for worker_id, port in self.worker_object:
            threading.Thread(target=self.create_worker, args=(worker_id, port)).start()


Solution()