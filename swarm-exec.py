#!/bin/env python3
import argparse
import datetime
import signal
import subprocess

parser = argparse.ArgumentParser(
    prog="Swarm-Exec",
    description="Execute a command on all nodes in the docker swarm mode.",
    add_help=True,
)

"""
Args:
--rm: Remove the container after execution.
--verbose | -v: Enable verbose output.
"""
parser.add_argument(
    "--rm", action="store_true", help="Remove the container after execution."
)
parser.add_argument(
    "-v", "--verbose", action="store_true", help="Enable verbose output."
)
parser.add_argument(
    "--logs", action="store_true", help="Show the logs of the container."
)
parser.add_argument(
    "command",
    type=str,
    help="Command to execute on swarm nodes.",
)
parser.add_argument(
    "--mode",
    type=str,
    choices=["global", "replicated"],
    default="global",
    help="Mode of the service.",
)


def cleanup(signum, frame):
    """Cleanup function to be called on SIGINT and SIGTERM signals."""

    # Remove the container on exit
    print("Cleaning up...")

    raise SystemExit(0)


# Register cleanup function
signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)


"""
docker service create \
    --name $container_name \
    --mode global \
    --cap-add=ALL \
    --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
    --restart-condition none \
    docker:cli sh -c "$command && while true; do sleep 3600; done"
"""


def exec_command(command: str, /, *, logs: bool = True):
    """Run a command"""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE if logs else None,
        stderr=subprocess.PIPE if logs else None,
        universal_newlines=True,
    )

    if logs:
        if process.stdout is not None:
            for stdout_line in iter(process.stdout.readline, ""):
                print("OUT: " + stdout_line, end="")

        if process.stderr is not None:
            for stderr_line in iter(process.stderr.readline, ""):
                print("ERR: " + stderr_line, end="")

    if process.stdout:
        process.stdout.close()
    if process.stderr:
        process.stderr.close()

    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def main():
    inputs = parser.parse_args()

    now = datetime.datetime.now(datetime.timezone.utc)
    container_name: str = f"swarm-exec_{now.isoformat(timespec='seconds')}"

    command_template = f"""
docker service create \
    --name {container_name}\
    --mode {inputs.mode} \
    --cap-add=ALL \
    --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
    --restart-condition none \
    docker:cli sh -c "{inputs.command} && while true; do sleep 3600; done"
"""

    print(command_template)

    print(f"Executing command: {inputs.command}")

    try:
        exec_command(inputs.command, logs=inputs.logs)
    except subprocess.CalledProcessError as e:
        print(e)

    # if inputs.rm:
    #     subprocess.run(
    #         f"docker rm -f {container_name}",
    #         shell=True,
    #         check=True,
    #         stdout=subprocess.DEVNULL if inputs.verbose else None,
    #         stderr=subprocess.DEVNULL if inputs.verbose else None,
    #     )


if __name__ == "__main__":
    main()
