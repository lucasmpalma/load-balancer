from random import randint
from threading import Thread

import time
import sys
import os
import pika

class Server:
    
    def __init__(self, id):
        print("> Creating Server.\n")
        
        self.id = id

        self.n_transaction = 0
        self.connection = 0
        self.channel = 0
    
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
                self.channel.basic_publish(exchange='', routing_key=servers_queue, body="S" + str(self.id) + "#" + str(self.n_transaction))
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
            
            print('[*] Balancer is waiting for messages. To exit press CTRL+C.\n')
            
            channel.start_consuming()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

    def addTransaction(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag = method.delivery_tag)
        self.n_transaction += 1
        print(f"[*] Server{self.id}(t): {self.n_transaction}.\n")

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