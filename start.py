from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import time
import subprocess

class ReloadHandler(FileSystemEventHandler):
    def __init__(self, flask_command):
        self.flask_command = flask_command
        self.process = None
        self.start_server()

    def start_server(self):
        if self.process:
            self.process.kill()
        self.process = subprocess.Popen(self.flask_command, shell=True)

    def on_any_event(self, event):
        if event.src_path.endswith(".py"):
            print(f"Change detected: {event.src_path}. Reloading server...")
            self.start_server()

if __name__ == "__main__":
    os.environ["FLASK_APP"] = "run.py"
    os.environ["FLASK_ENV"] = "development"

    path = "."
    handler = ReloadHandler(flask_command="flask run")
    observer = Observer()
    observer.schedule(handler, path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        if handler.process:
            handler.process.kill()
    observer.join()
