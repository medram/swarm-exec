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
parser.add_argument(
    "-q", "--quiet", action="store_true", help="Do not print the command template."
)
parser.add_argument(
    "-v", "--verbose", action="store_true", help="Enable verbose output."
)


def exec_command(command: str, /, *, print_output: bool = True) -> str:
    """Run a command asynchronously, returning output or error lines."""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    output: str = ""

    def stream_output(pipe, prefix):
        """Stream output from stdout or stderr asynchronously."""
        nonlocal output

        for line in iter(pipe.readline, ""):
            output += line
            print(f"{prefix}: {line}", end="")
        pipe.close()

    if print_output:
        if process.stdout:
            threading.Thread(target=stream_output, args=(process.stdout, "OUT")).start()
        if process.stderr:
            threading.Thread(target=stream_output, args=(process.stderr, "ERR")).start()

    # Wait for the command to complete without blocking the main thread
    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)

    return output


def number_of_replicas(service_name: str) -> int:
    return int(
        subprocess.check_output(
            'docker service ls --filter "name=%s" --filter "mode=global" --format "{{.Replicas}}" | cut -d"/" -f2'
            % service_name,
            shell=True,
            universal_newlines=True,
        )
    )


def output_container_logs(container_name: str, /, *, logs: bool = True) -> None:
    """Continuously checks logs for DOCKER_SWARM_COMMAND_STATUS=1 in container logs."""
    command = f"docker service logs {container_name} --follow"
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, universal_newlines=True
    )

    finished: int = 0

    replicas = number_of_replicas(container_name)
    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            if f"DOCKER_SWARM_COMMAND_STATUS=1" in line:
                finished += 1
            elif logs:
                print(f"LOG: {line}", end="")

            if finished >= replicas:
                print("Command finished.")
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

    # Register the cleanup function.
    def cleanup(signum, frame):
        """Cleanup function called on SIGINT and SIGTERM signals."""
        if inputs.verbose:
            print("Cleaning up...")
        exec_command(
            f"docker service rm {container_name}", print_output=not inputs.quiet
        )
        exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    if inputs.verbose:
        print("#" * 80)
        print(f"Executing command: {inputs.command}")
        print("#" * 80)
        print(f"Template command:\n{command_template}", end="")
        print("#" * 80)

    try:
        # Run the command template asynchronously
        exec_command(command_template, print_output=not inputs.quiet)
    except subprocess.CalledProcessError as e:
        print(e)

    # Check the container logs until DOCKER_SWARM_COMMAND_STATUS=1 is detected
    if inputs.logs:
        if inputs.verbose:
            print(f"Waiting for command to finish in container: {container_name}")
        output_container_logs(container_name, logs=inputs.logs)

    # Remove the container if specified
    if inputs.rm:
        time.sleep(1)
        if inputs.verbose:
            print(f"Removing container: {container_name}")
        exec_command(
            f"docker service rm {container_name}", print_output=not inputs.quiet
        )


if __name__ == "__main__":
    main()
