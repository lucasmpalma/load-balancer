from random import randint
from threading import Thread
from threading import RLock

import pika # RabbitMQ

import time
import sys
import os

class Server:
    
    def __init__(self, id):
        print(f"> Creating Server {id}.\n")
        
        self.id = id

        self.lock = RLock()

        self.transactions = []
        self.total = 0 # TOTAL TRANSACTIONS PROCESSED

        # TO CREATE A CONNECTION WITH BALANCER
        self.connection = 0
        self.channel = 0

    # --- SEND BALANCE TO BALANCER ---
    
    def openMsgQueue(self, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
    
    def closeMsgQueue(self):
        self.connection.close()
    
    def updateBalace(self, servers_queue):
        try:
            self.openMsgQueue(servers_queue)
            while(1):
                time.sleep(1)
                transac = len(self.transactions)*20
                self.channel.basic_publish(exchange='', routing_key=servers_queue, body="S" + str(self.id) + "#" + str(transac))
        except KeyboardInterrupt:
            self.closeMsgQueue()
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    # --- SEND BALANCE TO BALANCER ---

    # --- LISTEN TO BALANCER ---
    
    def listenBalancer(self, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            channel.basic_consume(queue=queue_name, on_message_callback=self.addTransaction)
            
            print(f"--- Server {self.id} is waiting for Balancer messages. To exit press CTRL+C.\n")
            
            channel.start_consuming()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

    def addTransaction(self, ch, method, properties, body):
        complexity = int(body.decode('utf-8')) # "Complexity"
        self.transactions.append(complexity)
        self.total += 1
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    # --- LISTEN TO BALANCER ---

    # --- RUN TRANSACTIONS ---
        
    def runTransaction(self):
        while(1):
            transaction = 0
            
            if len(self.transactions) > 0:
                transaction = self.transactions.pop()
            
            if transaction != 0: 
                print(f"--- Server {self.id} started running a Transaction of {transaction}s.\n")
                time.sleep(transaction)
                print(f"--- Server {self.id} ended running a Transaction of {transaction}s | Total Executed: {self.total}.\n")
    
    # --- RUN TRANSACTIONS ---

    def run(self, servers_queue):
        try:
            servers_thread = Thread(target = self.updateBalace, args = (servers_queue, ))
            servers_thread.start()

            balancer_thread = Thread(target = self.listenBalancer, args = ("S"+str(self.id), ))
            balancer_thread.start()

            run_thread = Thread(target = self.runTransaction)
            run_thread.start()

            balancer_thread.join()
            servers_thread.join()
            run_thread.join()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)