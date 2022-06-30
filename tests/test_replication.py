#!/usr/bin/env python3

import random
import sys
from time import sleep
from testsupport import subtest, info, warn, run
from socketsupport import run_client, run_server


def main(num_servers: int = 3,
         have_leader_at_startup: bool = True,
         kvs_port_start: int = 30000,
         raft_port_start: int = 1025,
         num_operations: int = 100,
         seed: int = 1024,
         debug: bool = False) -> None:
    with subtest("Testing replication"):
        random.seed(seed)

        info(f"num_servers = {num_servers}")
        info(f"have_leader_at_startup = {have_leader_at_startup}")
        info(f"kvs_port_start = {kvs_port_start}")
        info(f"raft_port_start = {raft_port_start}")

        raft_ports = []
        for i in range(num_servers):
            raft_ports.append(raft_port_start + i)

        info("start servers")
        servers = []
        for i in range(num_servers):
            leader = False
            if have_leader_at_startup and i == 0:
                leader = True
            servers.append(
                run_server(kvs_port_start + i, leader, raft_ports, i))

        # wait for completion of a leader election (if any)
        sleep(5)

        fail = False
        try:
            # find leader
            leader_port = -1
            if have_leader_at_startup:
                leader_port = kvs_port_start
            else:
                for i in range(num_servers):
                    port = kvs_port_start + i
                    ret, _ = run_client(port, "GET", "NON_EXIST_KEY", 0)
                    if ret == 3:  # no key
                        leader_port = port
                        break
            if leader_port == -1:
                fail = True

            if not fail:
                info(f"leader port: {leader_port}")

                kvs = []
                for i in range(num_operations):
                    key = random.randrange(10000)
                    value = random.randrange(10000)
                    ret, _ = run_client(leader_port, "PUT", key, value)
                    kvs.append((key, value))
                    if ret != 0:
                        info("client error")
                        fail = True
                        break

                # wait for commit (triggered by an empty AppendEntryRPC)
                sleep(2)

            if not fail:
                for k, v in kvs:
                    ret = 0
                    for i in range(num_servers):
                        ret, stdout = run_client(kvs_port_start + i,
                                                 "DIRECT_GET", k, v)
                        if ret != 0:
                            fail = True
                            break
                        value = stdout.strip()
                        if value != str(v):
                            fail = True
                            break
                    if fail:
                        info("client error")
                        break
        except Exception as e:
            warn(f"{e}")
            fail = True

        info("terminate servers")
        for server in servers:
            server.terminate()

        if fail:
            warn("test failed")
            sys.exit(1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_servers', type=int, default=3, required=False)
    parser.add_argument('--have_leader_at_startup',
                        action="store_true",
                        required=False)
    parser.add_argument('--kvs_port_start',
                        type=int,
                        default=30000,
                        required=False)
    parser.add_argument('--raft_port_start',
                        type=int,
                        default=1025,
                        required=False)
    parser.add_argument('--num_operations',
                        type=int,
                        default=100,
                        required=False)
    parser.add_argument('--seed', type=int, default=1024, required=False)
    parser.add_argument('--debug', action="store_true", required=False)
    args = parser.parse_args()
    main(args.num_servers, args.have_leader_at_startup, args.kvs_port_start,
         args.raft_port_start, args.num_operations, args.debug)
