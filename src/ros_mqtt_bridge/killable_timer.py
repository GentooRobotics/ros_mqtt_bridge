import sys
import threading
import time


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
