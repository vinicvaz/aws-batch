import sys
import json
import time

def train_job():
    request = json.loads(sys.argv[1])
    print('Job args', request)
    time.sleep(10)
    print('Job finished')