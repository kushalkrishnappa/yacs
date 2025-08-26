from datetime import datetime, time
from os import name
import pandas as pd
import statistics
import numpy as np
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

"""saved as logs.csv
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


class BarGraph:
    def __init__(self, logs_csv, algo_type: int) -> None:
        self.algo_type = algo_type
        self.logs_csv = logs_csv
        self.getAllData()
        self.GetJobsTasks()
        self.GetJobTime()
        self.GetTaskTime()
        self.plotBarGraph()

    def getAllData(self):
        self.algo_data = self.logs_csv[self.logs_csv["algo_type"] == self.algo_type]

    def GetJobsTasks(self):
        self.jobs = self.algo_data[self.algo_data["worker_id"] == -1]
        self.tasks = self.algo_data[self.algo_data["worker_id"] != -1]

    def GetJobTime(self):
        self.job_time_spent_list = []
        for i in self.jobs.groupby("id"):
            same_job = i[1]
            from_time = datetime.strptime(
                same_job.iloc[0:1, :]["timestamp"].values[0], r"%Y-%m-%d %H:%M:%S.%f"
            )
            to_time = datetime.strptime(
                same_job.iloc[-1:, :]["timestamp"].values[0], r"%Y-%m-%d %H:%M:%S.%f"
            )
            job_id = same_job.iloc[0:1, :]["id"].values[0]
            sec_spent = abs(from_time - to_time).seconds
            self.job_time_spent_list.append(sec_spent)
        self.job_mean_time = statistics.mean(self.job_time_spent_list)
        self.job_median_time = statistics.median(self.job_time_spent_list)
        print(self.job_mean_time, self.job_median_time)

    def GetTaskTime(self):
        self.task_time_spent_list = []
        for i in self.tasks.groupby("id"):
            same_task = i[1]
            from_time = datetime.strptime(
                same_task.iloc[0:1, :]["timestamp"].values[0], r"%Y-%m-%d %H:%M:%S.%f"
            )
            to_time = datetime.strptime(
                same_task.iloc[-1:, :]["timestamp"].values[0], r"%Y-%m-%d %H:%M:%S.%f"
            )
            task_id = same_task.iloc[0:1, :]["id"].values[0]
            sec_spent = abs(from_time - to_time).seconds
            self.task_time_spent_list.append(sec_spent)
        self.task_mean_time = statistics.mean(self.task_time_spent_list)
        self.task_median_time = statistics.median(self.task_time_spent_list)
        print(self.task_mean_time, self.task_median_time)

    def plotBarGraph(self):
        n_groups = 2
        median_time = (self.job_median_time, self.task_median_time)
        mean_time = (self.job_mean_time, self.task_mean_time)
        fig, ax = plt.subplots()
        index = np.arange(n_groups)
        bar_width = 0.35
        opacity = 0.8
        rects1 = plt.bar(
            index, mean_time, bar_width, alpha=opacity, color="b", label="MEAN"
        )
        rects2 = plt.bar(
            index + bar_width,
            median_time,
            bar_width,
            alpha=opacity,
            color="g",
            label="MEDIAN",
        )
        plt.ylabel("COMPLETION TIME ---->")
        plt.title("TASK AND JOB COMPLETION TIME")
        plt.xticks(index + (bar_width / 2), ("JOB", "TASK"))
        plt.legend()
        plt.tight_layout()
        plt.show()


class HeatMap:
    def __init__(self, logs_csv, algo_type: int) -> None:
        self.logs_csv = logs_csv.sort_values(by="timestamp")
        self.algo_type = algo_type
        self.GetAllData()
        self.GetTasks()
        self.createTable()

    def GetAllData(self):
        self.logs_csv = self.logs_csv[self.logs_csv["algo_type"] == self.algo_type]

    def GetTasks(self):
        self.tasks = self.logs_csv[self.logs_csv["worker_id"] != -1]

    def createTable(self):
        dikt = {1: 0, 2: 0, 3: 0}
        list_of_lists = []
        prev_second = 0
        self.initial_time = datetime.strptime(
            self.tasks["timestamp"].values[0], r"%Y-%m-%d %H:%M:%S.%f"
        )
        count = 0
        for row in self.tasks.itertuples(index=False):
            code = row[2]
            timestamp = datetime.strptime(row[4], r"%Y-%m-%d %H:%M:%S.%f")
            relative_sec = int(abs(timestamp - self.initial_time).seconds)
            worker_id = row[5]
            if prev_second != relative_sec:
                for xyz in range(1, 4):
                    list_of_lists.append([prev_second, xyz, dikt[xyz]])
                prev_second = relative_sec
            if code == 102:
                dikt[worker_id] += 1
            else:
                dikt[worker_id] -= 1
        for xyz in range(1, 4):
            list_of_lists.append([prev_second, xyz, dikt[xyz]])
        df = pd.DataFrame(list_of_lists)
        df.columns = ["second", "worker_id", "no_tasks"]
        df = df.pivot("worker_id", "second", "no_tasks")
        hm = sns.heatmap(df, annot=True, fmt="d")
        plt.title("HEATMAP")
        plt.show()


def tryexcept(logs_csv):
    for i in [1, 2, 3]:
        try:
            BarGraph(logs_csv, i)
            HeatMap(logs_csv, i)
        except:
            print(f"BarGraph for {i} did not execute")


logs_csv = pd.read_csv("logs.csv")
logs_csv.columns = ["sl_no", "algo_type", "code", "id", "timestamp", "worker_id"]
tryexcept(logs_csv)
