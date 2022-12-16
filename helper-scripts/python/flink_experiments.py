""" Script for running experiments testing the scalability of system with varying parallelism """
import requests
from time import sleep
from typing import List
import re

BASE_URL: str = "http://emu-23:11091"

LAMBDA_REGEX = r".*-l=(\d+[\d.]*).*"

JAR_ID = None

TIMER = None

TASK_MANAGERS = 10

SLOTS_PER_MANAGER = 40

N_TIMES_EACH = 3

ARGS = {
    "--tagsAskUbuntu:datasetType": "n2v",
    "--tagsAskUbuntu:streamType":"edge-stream",
    "-p": ["hdrf"],
    "-f": ["true"],
    "-d": ["tags-ask-ubuntu"],
    "-l": "3",
}


def get_jar_id():
    global JAR_ID
    response = requests.get("%s/jars" % BASE_URL)
    JAR_ID = response.json()['files'][0]['id']


def get_parallelisms(l: float) -> List[int]:
    result = []
    for i in range(1, TASK_MANAGERS + 1):
        slots = i * SLOTS_PER_MANAGER
        parallelism = int((slots - 2) // (l + 1))
        result.append(parallelism)
    return result


def get_all_configurations() -> List[str]:
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


def start_experiments(configurations: List[str]):
    for configuration in configurations:
        test = re.search(LAMBDA_REGEX, configuration)
        parallelisms = get_parallelisms(float(1 if test is None else test.group(1)))
        for parallelism in parallelisms:
            for trial in range(N_TIMES_EACH):
                data = {"programArgs": configuration,
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
                        break
                sleep(40)


start_experiments(get_all_configurations())

if __name__ == "__main__":
    get_jar_id()
    start_experiments()
