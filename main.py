# QUIX
# Vários servidores replicados;
# Transações simultânias;
# Servidore atualizam periodicamente sua carga (0-100);
# Requisições recebidas devem ser distribuídas aleatoriamente entre os N/2 servidores com menor carga;

# Transação: complexidade
# Servidor: Carga

import time
from threading import Thread

class Balancer:
    def __init__(self, servers):
        self.servers = servers

    def request(self, transactions):
        print(f"Balancer has {len(transactions)} new transactions.")
        self.servers[0].run(transactions)

    def get_servers(self):
        return len(self.servers)

class Transaction:
    def __init__(self, id, complexity):
        self.complexity = complexity
        self.id = id
    
    def request(self, balancer):
        balancer.request(self)
    
    def run(self):
        print(f"Starting transaction {self.id}.")
        time.sleep(self.complexity)
        print(f"Ending transaction {self.id}.")

class Server:
    def __init__(self, id):
        Thread.__init__(self)
        self.id = id
        self.transactions = []
        self.balance = len(self.transactions)

    def work(self, n):
        self.balance = self.balance + n
    
    def brodcast_balance(self):
        return self.balance
    
    def run(self, transactions):
        self.transactions = transactions
        for t in self.transactions:
            t.run()

def main(n_servers, n_transactions):
    servers = []
    trasactions = []

    for i in range(0,n_servers):
        servers.append(Server(i))
    balancer = Balancer(servers)
    print(balancer.get_servers())

    for i in range(0, n_transactions):
        trasactions.append(Transaction(i,3))
    
    balancer.request(trasactions)

main(3,3)