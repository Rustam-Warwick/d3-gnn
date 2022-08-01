""" Script for running experiments testing the scalability of system with varying parallelism """
import requests
from time import sleep

BASE_URL: str ="http://emu-23:11091"

JAR_ID = None

TASK_MANAGERS = 10

N_TIMES_EACH = 3

PARALLELISMS = []

l = 3

DATASETS = ["reddit-hyperlink"]

ARGS = "-o -p=hdrf -f"

TIMER = None

for i in range(1,TASK_MANAGERS + 1):
    slots = i * 40
    parallelism = (int)(slots - 2) // (l+1)
    PARALLELISMS.append(parallelism)

PARALLELISMS = list(reversed(PARALLELISMS))

def get_jar_id():
    global JAR_ID
    response = requests.get("%s/jars" % BASE_URL)
    JAR_ID = response.json()['files'][0]['id']


def start_experiments():
    for dataset in DATASETS:
        for parallelism in PARALLELISMS:
            for trial in range(N_TIMES_EACH):
                data = {"programArgs": "%s -d=%s -l=%.1f" % (ARGS, dataset, l),
                        "parallelism": parallelism,
                        "entryClass": "helpers.Main"
                        }
                response = requests.post("%s/jars/%s/run" % (BASE_URL, JAR_ID), json=data)
                job_id = response.json()['jobid']

                if TIMER:
                    sleep(TIMER)
                    requests.patch('%s/jobs/%s' % (BASE_URL, job_id))

                while True:
                    sleep(30)
                    job_response = requests.get("%s/jobs/%s" % (BASE_URL, job_id))
                    state = job_response.json()['state']
                    print(state)
                    if state == "CANCELED" or state == "FINISHED" or state == "FAILED":
                        break;
                sleep(40)


if __name__ == "__main__":
    get_jar_id()
    start_experiments()
