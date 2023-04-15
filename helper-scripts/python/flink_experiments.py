""" Script for running experiments testing the scalability of system with varying parallelism """
import requests
from time import sleep
import math
from typing import List
from threading import Thread, Lock
import re

BASE_URL: str = "http://emu-23:11091"

LAMBDA_REGEX = r".*-l=(\d+[\d.]*).*"

JAR_ID = None

TIMER = None

TASK_MANAGERS = 9

SLOTS_PER_MANAGER = 40

FREE_SLOTS = TASK_MANAGERS * SLOTS_PER_MANAGER

N_TIMES_EACH = 3

LOCK = Lock()

ARGS = {
    "-p": ["hdrf:buffered", "random:buffered", "clda:buffered"],
    "-f": ["true"],
    "-d": ["sx-superuser"],
    "-l": "3"
}


def get_jar_id():
    """ Get global JAR ID """
    global JAR_ID
    response = requests.get("%s/jars" % BASE_URL)
    JAR_ID = response.json()['files'][0]['id']


def get_parallelisms_and_slots_required(l: float) -> List[List[int]]:
    """ Given lambda get all parallelisms and slots possible """
    result = []
    for i in range(1, TASK_MANAGERS + 1):
        slots = i * SLOTS_PER_MANAGER
        parallelism = math.floor(slots / (l + 1))
        result.append([parallelism, slots])
    return result


def get_all_configurations() -> List[str]:
    """ Get a list of all configurations """
    configurations = [""]
    for argName, argValues in ARGS.items():
        if isinstance(argValues, list):
            new_configurations = []
            for argValue in argValues:
                new_configurations = [*new_configurations,
                                      *list(map(lambda a: f'{a} {argName}={argValue}', configurations))]
            configurations = new_configurations
        else:
            configurations = list(map(lambda a: f'{a} {argName}={argValues}', configurations))

    return configurations


def find_termination(job_id: str, slots_required: int):
    """" Find termination point for each experiment """
    global FREE_SLOTS
    if TIMER is not None:
        sleep(TIMER)
        requests.patch('%s/jobs/%s' % (BASE_URL, job_id))
    while True:
        sleep(10)
        job_response = requests.get("%s/jobs/%s" % (BASE_URL, job_id))
        state = job_response.json()['state']
        print(state)
        if state == "CANCELED" or state == "FINISHED" or state == "FAILED":
            break
    sleep(60)
    with LOCK:
        FREE_SLOTS += slots_required


def start_experiments(configurations: List[str]):
    """ Do the experiments """
    global FREE_SLOTS
    for configuration in configurations:
        test = re.search(LAMBDA_REGEX, configuration)
        parallelisms_and_slots = get_parallelisms_and_slots_required(float(1 if test is None else test.group(1)))
        for (parallelism, slots_required) in parallelisms_and_slots:
            for trial in range(N_TIMES_EACH):
                sleep(10)
                while FREE_SLOTS < slots_required:
                    sleep(1)
                data = {"programArgs": configuration,
                        "parallelism": parallelism,
                        "entryClass": "helpers.Main"
                        }
                response = requests.post("%s/jars/%s/run" % (BASE_URL, JAR_ID), json=data)
                job_id = response.json()['jobid']
                with LOCK:
                    FREE_SLOTS -= slots_required
                Thread(target=find_termination, args=(job_id, slots_required)).start()


if __name__ == "__main__":
    get_jar_id()
    start_experiments(get_all_configurations())
