import sys
import threading
import time

import requests


class KillableTimer(threading.Timer):
    def __init__(self, *args, daemon=False, **kwargs):
        threading.Timer.__init__(self, *args, **kwargs)
        self.daemon = daemon
        self.killed = threading.Event()

    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        threading.Timer.start(self)

    def __run(self):
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, event, arg):
        if event == "call":
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, event, arg):
        if self.killed.is_set():
            if event == "line":
                raise SystemExit()
            return self.localtrace

    def kill(self):
        self.killed.set()


if __name__ == "__main__":

    def download_file(url):
        local_filename = url.split("/")[-1]
        # NOTE the stream=True parameter below
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    # if chunk:
                    print("downloading")
                    f.write(chunk)
        return local_filename

    def func():
        download_file("https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz")

    t1 = KillableTimer(2, func)
    t1.start()
    start_time = time.time()
    while time.time() - start_time < 5:
        time.sleep(1)
        print("Ticking time")
    t1.kill()
    t1.join()
    if not t1.is_alive():
        print("Thread killed")