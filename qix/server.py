from random import randint
from threading import Thread
from threading import RLock

import time
import sys
import os
import pika

class Server:
    
    def __init__(self, id):
        print("> Creating Server.\n")
        
        self.id = id

        self.lock = RLock()

        self.n_transaction = 0
        self.connection = 0
        self.channel = 0

        self.total = 0
    
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
                transac = 0
                with self.lock:
                    transac = self.n_transaction
                self.channel.basic_publish(exchange='', routing_key=servers_queue, body="S" + str(self.id) + "#" + str(transac))
        except KeyboardInterrupt:
            self.closeMsgQueue()
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    def listenBalancer(self, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            channel.basic_consume(queue=queue_name, on_message_callback=self.addTransaction)
            
            print(f"--- Server {self.id} is waiting for messages. To exit press CTRL+C.\n")
            
            channel.start_consuming()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    def updateLine(self, value):
        with self.lock:
            self.n_transaction += value
    
    def getLine(self):
        with self.lock:
            return self.n_transaction

    def addTransaction(self, ch, method, properties, body):
        update_thread = Thread(target = self.updateLine, args = (1, ))
        update_thread.start()
        update_thread.join()

        complexity = int(body.decode('utf-8'))
        
        ch.basic_ack(delivery_tag = method.delivery_tag)

        time.sleep(complexity)
        
        # with self.lock:
        #     self.n_transaction -= 1
        #     self.total += 1
        
        # ch.basic_ack(delivery_tag = method.delivery_tag)
        
        print(f"--- Server {self.id} ended a Transaction of {complexity} seconds | Total: {self.total}.\n")
        
    def run(self, servers_queue):
        try:
            servers_thread = Thread(target = self.updateBalace, args = (servers_queue, ))
            servers_thread.start()

            balancer_thread = Thread(target = self.listenBalancer, args = ("S"+str(self.id), ))
            balancer_thread.start()

            balancer_thread.join()
            servers_thread.join()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)