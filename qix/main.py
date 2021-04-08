from client import Client
from balancer import Balancer
from server import Server

from multiprocessing import Process

import sys
import os

def main(n_servers):
    client_queue = "network"
    servers_queue = "servers"

    try:
        b = Balancer()
        b_process = Process(target=b.run, args=(client_queue,servers_queue,))
        b_process.start()

        server_processes = []
        for i in range(0, n_servers):
            s = Server(i)
            s_process = Process(target=s.run, args=(servers_queue,))
            server_processes.append(s_process)
            s_process.start()

        c = Client(client_queue)
        c_process = Process(target=c.genTransactions, args=(1,3,25))
        c_process.start()

        for s in server_processes:
            s.join()

        b_process.join()
        c_process.join()

    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

# ---

main(3)