from client import Client
from balancer import Balancer
from multiprocessing import Process

import sys
import os

def main():
    queue_name = "network"
    try:
        b = Balancer(queue_name)

        b_process = Process(target=b.listenClient)
        b_process.start()

        c = Client(queue_name)

        c_process = Process(target=c.genTransactions, args=(1,3,))
        c_process.start()

        b_process.join()
        c_process.join()

    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

# ---

main()