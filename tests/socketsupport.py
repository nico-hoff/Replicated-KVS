from subprocess import Popen
import tempfile
import sys
import subprocess
import time
import psutil

from testsupport import (
    run_project_executable,
    info,
    warn,
    find_project_executable,
)


def is_server_listening(port: int) -> bool:
    for conn in psutil.net_connections():
        if conn.laddr.port == port and conn.status == "LISTEN":
            return True
    return False


def run_client(port: int, operation: str, key: int, value: int) -> int:
    info(f"Running client.")

    with tempfile.TemporaryFile(mode="w+") as stdout:
        proc = run_project_executable("clt",
                                      args=[
                                          "-p",
                                          str(port),
                                          "-o",
                                          operation,
                                          "-k",
                                          str(key),
                                          "-v",
                                          str(value),
                                      ],
                                      stdout=stdout,
                                      check=False)

        ret = proc.returncode
        stdout.seek(0)
        out = stdout.read()

        return ret, out


def wait_unbound(port: int):
    while is_server_listening(port):
        info("Waiting for port to be unbound")
        time.sleep(5)


def run_server(port: int, leader: bool, raft_ports: [int],
               server_id: int) -> Popen:
    try:
        wait_unbound(port)
        wait_unbound(raft_ports[server_id])

        server = [
            find_project_executable("svr"),
            "-p",
            str(port),
            "-r",
            ",".join(map(str, raft_ports)),
            "-i",
            str(server_id),
        ]
        if leader:
            server += ["--leader"]

        info(f"Run server {server_id}")

        print(" ".join(server))
        info(f"{server}")
        proc = subprocess.Popen(
            server,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return proc
    except OSError as e:
        warn(f"Failed to run command: {e}")
        sys.exit(1)
