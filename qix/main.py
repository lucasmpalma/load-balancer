from client import Client
from balancer import Balancer
from server import Server

from multiprocessing import Process

import sys
import os

def main(n_servers, n_transactions):

    # REMEMBER TO RUN THE DOCKER IMAGE (RabbitMQ): docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

    client_queue = "network"
    servers_queue = "servers"

    try:

        # --- START BALANCER PROCESS
        b = Balancer()
        b_process = Process(target=b.run, args=(client_queue,servers_queue,))
        b_process.start()

        # --- START SERVERS PROCESSES
        server_processes = []
        for i in range(0, n_servers):
            s = Server(i)
            s_process = Process(target=s.run, args=(servers_queue,))
            server_processes.append(s_process)
            s_process.start()

        # --- START CLIENT PROCESS
        c = Client(client_queue)
        c_process = Process(target=c.genTransactions, args=(1,2,n_transactions))
        c_process.start()

        # --- JOIN PROCESSES
        for s in server_processes:
            s.join()

        b_process.join()
        c_process.join()

    except KeyboardInterrupt:

        # HANDLE CTRL+C
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

# ---

main(5,50)