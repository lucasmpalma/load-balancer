from threading import Thread
from random import randint
import queue
import time

class Balancer():

    def __init__ (self, n_servers):
        self.n_servers = n_servers
        self.servers = []
        self.listener = Listener()

    def init_servers(self):
        for s in range (0,self.n_servers):
            server = Server(s)
            self.servers.append(server)
            server.start()
        self.listener.start()
    
    def run(self):
        while(1):
            pending = self.listener.get_pending()
            if pending != None:
                self.servers[randint(0,2)].add_transaction(pending)

class Listener(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.pending_transactions = queue.Queue()
    
    def get_pending(self):
        pending = self.pending_transactions.get()
        self.pending_transactions.task_done()
        self.pending_transactions.join()	
        return pending

    def run(self):
        n_transactions = 0 # número total de transações executadas.
        while(1):
            time.sleep(randint(1,7)) # tempo até a próxima transação na rede.
            self.pending_transactions.put(Transaction(n_transactions,randint(1,7))) # criada nova transação com complexidade entre (1,7) segundos.
            n_transactions = n_transactions + 1
            print(f"Pending Transactions: {n_transactions}.")
    
class Transaction:

    def __init__(self, id, complexity):
        self.id = id
        self.complexity = complexity
    
    def run(self, server_id):
        print(f"Server {server_id} running Transaction {self.id}.")
        time.sleep(self.complexity)
        print(f"Transaction {self.id} executed by Server {server_id}.")

class Server(Thread):
    
    def __init__(self, id):
        Thread.__init__(self)
        self.id = id
        self.transactions = queue.Queue()
        self.load = self.transactions.qsize()
    
    def broadcast_load(self):
        while(1):
            time.sleep(3)
            print(f"Server {self.id} load: {self.load}.")
    
    def add_transaction(self, transaction):
        self.transactions.put(transaction) #  Balancer adiciona uma Transação à queue de transações do Servidor.
    
    def run(self):
        print(f"Server {self.id} is ready.")
        while(1):
            if self.transactions.qsize() > 0: # Servidor verifica se possui transações na sua queue.
                t = self.transactions.get() # Servidor sequencialmente as transações em sua queue.
                self.transactions.task_done()
                self.transactions.join()	
                t.run(self.id) # Servidor começa a execução da Transação.

# ---

def main():
    n_servers = 3
    b = Balancer(n_servers)
    b.init_servers()
    b.run()

main()