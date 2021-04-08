from threading import Thread

import pika # RabbitMQ

import sys
import os
import random
import datetime

class Balancer:

    def __init__(self):
        print("[*] Creating Balancer.\n")

        self.trasactions = []
        self.balances = {} # {"S0":0} NUMBER OF TRANSACTIONS FOR EACH SERVER
        self.capacities = {} # {"S0":0} CAPACITY FOR EACH SERVER
        self.updates = {} # {"S0":0} TIME OF THE LAST UPDATE FOR EACH SERVER

        # --- TO CREATE CONNECTIONS TO SEND TRANSACTIONS TO SERVERS
        self.connection = 0
        self.channel = 0

    # --- LISTEN TO CLIENT ---

    def listenClient(self, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            channel.basic_consume(queue=queue_name, on_message_callback=self.addTransaction)
            
            print('[*] Balancer is waiting for Client messages. To exit press CTRL+C.\n')
            
            channel.start_consuming()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    def addTransaction(self, ch, method, properties, body):
        server_msg = body.decode('utf-8') # "Transaction Number#Complexity"
        complexity = int(server_msg.split("#")[1])
    
        self.trasactions.append(int(complexity))

        ch.basic_ack(delivery_tag = method.delivery_tag)
        
        print(f"[*] Balancer received: {server_msg} |  Transactions: {self.trasactions}.\n")

    # --- LISTEN TO CLIENT ---

    # --- LISTEN TO SERVERS ---
   
    def listenServers(self, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            channel.basic_consume(queue=queue_name, on_message_callback=self.updateBalaces)
            
            print('[*] Balancer is waiting for Servers messages. To exit press CTRL+C.\n')
            
            channel.start_consuming()
        except KeyboardInterrupt:
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

    def updateBalaces(self, ch, method, properties, body):
        server_msg = body.decode('utf-8').split("#") # "SID#Number#Capacity"
        self.balances[str(server_msg[0])] = int(server_msg[1])
        self.capacities[str(server_msg[0])] = int(server_msg[2])
        self.updates[str(server_msg[0])] = datetime.datetime.now()
        
        ch.basic_ack(delivery_tag = method.delivery_tag)

        print(f"[*] Balances: {self.balances}.\n")
    
    # --- LISTEN TO SERVERS ---

    # --- SEND MESSAGES TO SERVERS ---

    def openMsgQueue(self, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
    
    def closeMsgQueue(self):
        self.connection.close()

    def sendTransaction(self):
        try:
            while(1):
                n_servers = len(self.balances.keys())
                if n_servers > 0:
                    # --- SERVER SELECTION
                    
                    ordered_balances = dict(sorted(self.balances.items(), key=lambda item: item[1]))
                    candidates = list(ordered_balances)[:int(n_servers/2)]
                    
                    selected = 0
                    if len(candidates) > 0:
                        selected = random.choice(candidates)
                    else:
                        selected = str(min(self.balances.items(), key=lambda x: x[1])[0])

                    self.openMsgQueue(selected)
                    transaction = ""

                    time_diff = self.updates[selected] - datetime.datetime.now()

                    if len(self.trasactions) > 0 and self.balances[selected] < self.capacities[selected] and time_diff.seconds > 3:
                        transaction = self.trasactions.pop()
                        self.channel.basic_publish(exchange='', routing_key=selected, body=str(transaction))
        except KeyboardInterrupt:
            self.closeMsgQueue()
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
    
    # --- SEND MESSAGES TO SERVERS ---

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