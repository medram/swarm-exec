#!/bin/env python3

import argparse
import datetime
import os
import signal
import subprocess
import threading
import time

# Initialize parser
parser = argparse.ArgumentParser(
    prog="Swarm-Exec",
    description="Execute a command on all nodes in Docker Swarm mode.",
    add_help=True,
)
parser.add_argument(
    "--rm", action="store_true", help="Remove the container after execution."
)
parser.add_argument(
    "-v", "--verbose", action="store_true", help="Enable verbose output."
)
parser.add_argument(
    "--logs", action="store_true", help="Show the logs of the container."
)
parser.add_argument("command", type=str, help="Command to execute on swarm nodes.")
parser.add_argument(
    "--mode",
    type=str,
    choices=["global", "replicated"],
    default="global",
    help="Mode of the service.",
)


def exec_command(command: str, /, *, logs: bool = True):
    """Run a command asynchronously, returning output or error lines."""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE if logs else None,
        stderr=subprocess.PIPE if logs else None,
        universal_newlines=True,
    )

    def stream_output(pipe, prefix):
        """Stream output from stdout or stderr asynchronously."""
        for line in iter(pipe.readline, ""):
            print(f"{prefix}: {line}", end="")
        pipe.close()

    if logs:
        if process.stdout:
            threading.Thread(target=stream_output, args=(process.stdout, "OUT")).start()
        if process.stderr:
            threading.Thread(target=stream_output, args=(process.stderr, "ERR")).start()

    # Wait for the command to complete without blocking the main thread
    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def wait_for_command_finish(container_name):
    """Continuously checks logs for DOCKER_SWARM_COMMAND_STATUS=1 in container logs."""
    command = f"docker service logs {container_name} --follow"
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, universal_newlines=True
    )

    for line in iter(process.stdout.readline, ""):
        print(f"LOG: {line}", end="")
        if "DOCKER_SWARM_COMMAND_STATUS=1" in line:
            print("Command has finished.")
            process.terminate()
            break

    process.stdout.close()
    process.wait()


def main():
    inputs = parser.parse_args()
    now = datetime.datetime.now(datetime.timezone.utc)
    container_name = (
        f"swarm-exec_{now.strftime('%Y-%m-%d_%H%M%S')}_{os.urandom(4).hex()}"
    )

    command_template = f"""
docker service create \
    --name {container_name} \
    --mode {inputs.mode} \
    --cap-add=ALL \
    --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
    --restart-condition none \
    docker:cli sh -c "{inputs.command} ; echo DOCKER_SWARM_COMMAND_STATUS=1 && while true; do sleep 3600; done"
"""

    def cleanup(signum, frame):
        """Cleanup function called on SIGINT and SIGTERM signals."""
        print("Cleaning up...")
        exec_command(f"docker service rm {container_name}", logs=inputs.logs)
        exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    print("#" * 80)
    print(f"Executing command: {inputs.command}")
    print("#" * 80)
    print(f"Template command:\n{command_template}", end="")
    print("#" * 80)

    try:
        # Run the command template asynchronously
        exec_command(command_template, logs=inputs.logs)
    except subprocess.CalledProcessError as e:
        print(e)

    # Check the container logs until DOCKER_SWARM_COMMAND_STATUS=1 is detected
    if inputs.logs:
        print(f"Waiting for command to finish in container: {container_name}")
        wait_for_command_finish(container_name)

    # Remove the container if specified
    if inputs.rm:
        time.sleep(1)
        print(f"Removing container: {container_name}")
        exec_command(f"docker service rm {container_name}", logs=inputs.logs)


if __name__ == "__main__":
    main()
