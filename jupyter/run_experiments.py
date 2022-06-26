import requests
from time import sleep

BASE_URL: str = "http://localhost:8000"
JAR_ID: str
PARALLELISMS = [5, 15, 45, 75, 105, 135, 165]
DATASETS = ["sx-stackoverflow-a2q-processed_1m.csv",
            "sx-stackoverflow-a2q-processed_5m.csv",
            "sx-stackoverflow-a2q-processed_10m.csv",
            "sx-stackoverflow-a2q-processed_15m.csv",
            "sx-stackoverflow-a2q-processed.csv"]
ARGS = "-o -p=hdrf -l=1.4 -f"

def get_job_id():
    global JAR_ID
    response = requests.get("%s/jars" % BASE_URL)
    JAR_ID = response.json()['files'][0]['id']


def start_experiments():
    for dataset in DATASETS:
        for parallelism in PARALLELISMS:
            data = {"programArgs": "%s -d=%s" % (ARGS, dataset),
                    "parallelism": parallelism,
                    "entryClass": "helpers.Main"
                    }
            response = requests.post("%s/jars/%s/run" % (BASE_URL, JAR_ID), json=data)
            job_id = response.json()['jobid']
            while True:
                sleep(30)
                job_response = requests.get("%s/jobs/%s" % (BASE_URL, job_id))
                state = job_response.json()['state']
                if state == "FINISHED":
                    break;
            sleep(30)



if __name__ == "__main__":
    get_job_id()
    start_experiments()
