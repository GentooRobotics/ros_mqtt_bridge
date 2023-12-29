import sys
import threading
import time

class KillableTimer(threading.Timer):
    def __init__(self, *args, interval = 0.0, daemon=False, timeout = None, **kwargs):
        kwargs["interval"] = interval
        threading.Timer.__init__(self, *args, **kwargs)
        self.daemon = daemon
        self.start_time = None
        self.killed = threading.Event()
        self.timeout = timeout

    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        self.start_time = time.time()
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

    def is_timed_out(self):
        if self.start_time is None or self.timeout is None:
            return False
        return time.time() - self.start_time > self.timeout