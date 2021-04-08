from threading import Thread
from threading import RLock

import pika
import sys
import os

class Balancer:

    def __init__(self):
        print("[*] Creating Balancer.\n")

        self.lock = RLock()

        self.trasactions = []
        self.balances = {}

        self.connection = 0
        self.channel = 0
    
    def listenClient(self, queue_name):
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
        server_msg = body.decode('utf-8')
        complexity = int(server_msg.split("#")[1])
        with self.lock:
            self.trasactions.append(int(complexity))

        ch.basic_ack(delivery_tag = method.delivery_tag)
        
        print(f"[*] Balancer received: {server_msg} |  Transactions: {self.trasactions}.\n")
   
    def listenServers(self, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            channel.basic_consume(queue=queue_name, on_message_callback=self.updateBalaces)
            
            print('[*] Balancer is waiting for messages. To exit press CTRL+C.\n')
            
            channel.start_consuming()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

    def updateBalaces(self, ch, method, properties, body):
        server_msg = body.decode('utf-8').split("#")
        self.balances[str(server_msg[0])] = int(server_msg[1])
        
        ch.basic_ack(delivery_tag = method.delivery_tag)

        print(f"[*] Balances: {self.balances}.\n")

    def openMsgQueue(self, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
    
    def closeMsgQueue(self):
        self.connection.close()

    def sendTransaction(self):
        try:
            while(1):
                if len(self.balances.keys()) > 0:
                    selected = str(min(self.balances.items(), key=lambda x: x[1])[0])
                    self.openMsgQueue(selected)
                    transaction = ""
                    with self.lock:
                        if len(self.trasactions):
                            transaction = self.trasactions.pop()
                    if transaction != "":
                        self.channel.basic_publish(exchange='', routing_key=selected, body=str(transaction))
        except KeyboardInterrupt:
            self.closeMsgQueue()
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

    def run(self, client_queue, servers_queue):
        try:
            client_thread = Thread(target = self.listenClient, args = (client_queue, ))
            client_thread.start()

            servers_thread = Thread(target = self.listenServers, args = (servers_queue, ))
            servers_thread.start()

            sender_thread = Thread(target = self.sendTransaction)
            sender_thread.start()
            
            client_thread.join()
            servers_thread.join()
            sender_thread.join() 
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)