import os
import signal
import subprocess

import click
import psutil
from rich_click import RichGroup

from . import CONTEXT_SETTINGS

PID_FILE = "fastapi_pid.txt"


def get_pid_from_file():
    if os.path.exists(PID_FILE):
        with open(PID_FILE, "r") as f:
            return int(f.read().strip())
    return None


def process_is_running():
    pid = get_pid_from_file()
    if pid is None:
        return False
    try:
        p = psutil.Process(pid)
        p.status()  # Throws an exception if the process is not running
        return True
    except psutil.NoSuchProcess:
        return False


def start_fastapi():
    if process_is_running():
        print("FastAPI is already running.")
        return

    print("Starting FastAPI...")
    process = subprocess.Popen(
        ["uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8000"]
    )

    with open(PID_FILE, "w") as pid_file:
        pid_file.write(str(process.pid))

    print(f"FastAPI started with PID {process.pid}")


def stop_fastapi():
    pid = get_pid_from_file()
    if pid is None or not process_is_running():
        print("FastAPI is not running.")
        return

    print(f"Stopping FastAPI with PID {pid}...")
    os.kill(pid, signal.SIGTERM)
    os.remove(PID_FILE)
    print(f"FastAPI with PID {pid} stopped.")


@click.group(
    name="server",
    cls=RichGroup,
    invoke_without_command=False,
    context_settings=CONTEXT_SETTINGS,
)
def cli():
    """CLI to manage FastAPI web service."""
    pass


@cli.command()
def start():
    """Start the FastAPI web service."""
    start_fastapi()


@cli.command()
def stop():
    """Stop the FastAPI web service."""
    stop_fastapi()


if __name__ == "__main__":
    cli()
