import time
from tqdm import tqdm


def wait_second(sec=60):
    time.sleep(1)
    for _ in tqdm(range(sec)):
        time.sleep(1)
