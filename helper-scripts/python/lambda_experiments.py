""" Script for running experiments with varying lambda(explosion factor) """
import requests
from time import sleep

BASE_URL: str = "http://emu-23:11091" # Job Manager

JAR_ID: str # Empty

SLOTS = 398 # Number of task slots available - 2

N_TIMES_EACH = 3 # How many times to run each experiment

lambdas = list() # List of lambdas to test from

DATASETS = ["stackoverflow"] # Datasets to test on

ARGS = "-o -p=hdrf -f" # Initial Job Parameters

TIMER = None # Timer after which to stop the execution and go for new one, If None will wait till finish


# Get the lambda values

l=1
while l <= 10:
    lambdas.append(l)
    l = l + 1

def get_jar_id():
    global JAR_ID
    response = requests.get("%s/jars" % BASE_URL)
    JAR_ID = response.json()['files'][0]['id']


def start_experiments():
    for dataset in DATASETS:
        for l in lambdas:
            for trial in range(N_TIMES_EACH):
                parallelism = SLOTS / (1 + l)
                data = {"programArgs": "%s -d=%s -l=%.1f" % (ARGS, dataset, l),
                        "parallelism": int(parallelism),
                        "entryClass": "helpers.Main"
                        }
                response = requests.post("%s/jars/%s/run" % (BASE_URL, JAR_ID), json=data)
                job_id = response.json()['jobid']

                if TIMER is not None:
                    sleep(TIMER)
                    requests.patch('%s/jobs/%s' % (BASE_URL, job_id))

                while True:
                    sleep(30)
                    job_response = requests.get("%s/jobs/%s" % (BASE_URL, job_id))
                    state = job_response.json()['state']
                    print(state)
                    if state == "CANCELED" or state == "FINISHED" or state == "FAILED":
                        break;
                sleep(60)

if __name__ == "__main__":
    get_jar_id()
    start_experiments()