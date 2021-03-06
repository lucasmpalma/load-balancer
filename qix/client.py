from random import randint

import pika # RabbitMQ

import time
import sys
import os

class Client:
    
    def __init__(self, queue_name):
        print("> Creating Client App.\n")
        
        self.n_transaction = 0
        
        # TO CREATE A CONNECTION WITH BALANCER
        self.connection = 0
        self.channel = 0
        self.queue_name = str(queue_name)
    
    # --- SEND TRANSACTION TO BALANCER ---

    def openMsgQueue(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
    
    def closeMsgQueue(self):
        self.connection.close()
    
    def genTransactions(self, upper, lower, limit):
        try:
            self.openMsgQueue()
            while(self.n_transaction < limit):
                slept = randint(upper,lower)
                time.sleep(slept)
                self.channel.basic_publish(exchange='', routing_key=self.queue_name, body="Transaction " + str(self.n_transaction) + "#" + str(randint(7,13)))
                print(f"> Client created Transaction: {self.n_transaction} (slept {slept}s).\n")
                self.n_transaction = self.n_transaction + 1
        except KeyboardInterrupt:
            self.closeMsgQueue()
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    # --- SEND TRANSACTION TO BALANCER ---